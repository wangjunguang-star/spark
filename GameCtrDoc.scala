package com.yy.hago.push.doc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random
import scala.collection.mutable.ArrayBuffer

object GameCtrDoc {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//        conf.set("spark.kryo.registrationRequired", "true")
        conf.registerKryoClasses(Array(classOf[CtrStruct], classOf[SelfFriendUid], classOf[MessageObj], classOf[RawCtrObj],classOf[CtrJson], classOf[UserProfile]))

        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder().enableHiveSupport()
          .config("spark.hadoop.validateOutputSpecs", value = true)
          .getOrCreate()

        val curInputPathGameCtr = args(0)      // hago ctr预估结果路径
        val beforeHavePushFriend = args(1)     // 最近两周给用户推荐过的好友
        val totalUserInputPath = args(2)       // 用户信息表
        val docInputPath = args(3)             // 文案表
        val gameInfoPath = args(4)             // 游戏画像路径
        val outputPathGameCtr = args(5)        // 配文案后的输出路径
        val topN = args(6).toInt               // topN参数

        // 读取最近14天给用户推荐过的friend_uid
        val rddCtrTowWeekBefore: RDD[SelfFriendUid] = readRecommondFriendUidV2(sc, beforeHavePushFriend)

        // 读取当天产出ctr预估数据
        val rddRawCtrData: RDD[RawCtrObj] = readRawCtrData(sc, curInputPathGameCtr)

        // TODO 过滤前置到此处
        // TODO val rddFilter: RDD[RawCtrObj] = filterOutRecentRecomFriend(rddRawCtrData, rddCtrTowWeekBefore)

        // 读取全量用户数据
        val rddTotalUser: RDD[UserProfile] = readTotalUsersFromHdfs(sparkSession, totalUserInputPath)

        // 读取游戏信息
        val rddGameInfo: RDD[(String, String)] = readGameInfoFromHdfs(sc, gameInfoPath)
        val rddGameInfoMap  = rddGameInfo.map(row=>{(row._1, row._2)}).collectAsMap()
        val bcGameInfo = sc.broadcast(rddGameInfoMap)

        // ctr数据 join 用户信息数据, 并填充相应的 CtrStruct 结构
        val rddCtrUserTotalInfo: RDD[CtrStruct] = ctrDataJoinUserData(rddRawCtrData, rddTotalUser)
        val rddCtrUserTotalInfoWithGameInfo = rddCtrUserTotalInfo.map(r=>{
            val game_id = r.game_id
            r.game_type = bcGameInfo.value.getOrElse(game_id, "9").toString
            r
        })

        // 过滤14天之内的重复推荐
        val rddFilter: RDD[CtrStruct] = filterOutAndSelectTopN(rddCtrUserTotalInfoWithGameInfo, rddCtrTowWeekBefore, topN)

        // 读取文案列表
        val rddCtrGameMessages: RDD[MessageObj] = readMessageObjFromHdfs(sc, docInputPath)
        val ctrGameMessages: Array[MessageObj] = rddCtrGameMessages.collect()
        val bcCtrGameMessage: Broadcast[Array[MessageObj]] = sparkSession.sparkContext.broadcast(ctrGameMessages)
        // 组装文案
        val rddCtrGameDoc : RDD[CtrStruct] = rddFilter.map( r => matchMessage(r, bcCtrGameMessage.value))
          .filter(r=>r != null)
        val rddPushDocs: RDD[(String, String)] = rddCtrGameDoc.map(r => (r.hdid, r.outputJson()))
          .groupByKey()
          .mapValues(values => {
            val jsonDocs: JSONArray = new JSONArray()
            for( v <- values.toList) {
                jsonDocs.add(v)
            }
            jsonDocs.toJSONString
        })
        rddPushDocs.map(r => s"${r._1}\t${r._2}").repartition(500).saveAsTextFile(outputPathGameCtr)
    }

    def matchMessage(recallStruct: CtrStruct, socialGameMessages: Array[MessageObj]): CtrStruct = {
        val messages: List[MessageObj] = Random.shuffle(socialGameMessages.toList)  // 随机配文案
        val messageIter: Iterator[MessageObj] = messages.toIterator
        var messageReady: Boolean = false
        while (messageIter.hasNext && !messageReady && !recallStruct.isDocValid()) {
            messageReady = false
            val message = messageIter.next()
            if (recallStruct.isMatched(message)) {
                recallStruct.initPlaceholders()
                val title = PlaceholderDoc.make(message.title, recallStruct.placeholders)
                val content = PlaceholderDoc.make(message.content, recallStruct.placeholders)
                if (title != null && content != null) {
                    recallStruct.assemble(message)
                    recallStruct.doc_title = title
                    recallStruct.doc_content = content
                    messageReady = true
                }
            }
        }
        if (recallStruct.isDocValid()) {
            recallStruct
        } else {
            null
        }
    }

    /**
     * 读取ctr预估数据
     * @param sc Spark 运行环境
     * @param path ctr预估数据的路径
     * @return DataFrame
     */
    def readRawCtrData(sc: SparkContext, path: String): RDD[RawCtrObj] = {
        val rddRawCtrdata = sc.textFile(path).map( row => {
            val parts = row.split("\t", -1)
            if(parts.size != 2) {
                null
            } else {
                val hdid = parts(0)
                val pctr_context = parts(1)
                val rawCtrData: RawCtrObj = new RawCtrObj()
                rawCtrData.hdid = hdid
                rawCtrData.pctr_context = pctr_context
                rawCtrData
            }

        }).filter( p=>p!=null)
        rddRawCtrdata
    }

    /**
     * 读取hago_dwd_user_uid_total_detail表,hive sql读取失败, 从hdfs直接读取数据
     * @param sparkSession
     * @param path
     * @param maxSilentDays
     * @return
     */
    def readTotalUsersFromHdfs(sparkSession: SparkSession, path: String, maxSilentDays: Int = 90) : RDD[UserProfile] = {
        val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val dateBefore90 = Calendar.getInstance()
        dateBefore90.add(Calendar.DATE, -maxSilentDays)
        val dtLastLoginDate = timeFormat.format(dateBefore90.getTime)

        val rddTotalUser = sparkSession.read.orc(path).rdd.map(row => {

            val userProfile: UserProfile = new UserProfile()
            userProfile.devId = row.getAs[String]("hdid")
            userProfile.country = row.getAs[String]("country_code")
            val dateFormat = new SimpleDateFormat("yyyyMMdd")
            userProfile.createTime = dateFormat.parse(row.getAs[String]("reg_date"))
            userProfile.updateTime = dateFormat.parse(row.getAs[String]("last_login_date"))
            userProfile.os = row.getAs[String]("mbos")
            userProfile.age = row.getAs[Int]("age")
            userProfile.sex = row.getAs[Int]("sex")
            userProfile.nickname = row.getAs[String]("nick")
            userProfile.mbl = row.getAs[String]("mbl")
            userProfile.uid = row.getAs[Long]("uid").toString

            val lastLoginDate = row.getAs[String]("last_login_date")
            val ver = row.getAs[String]("ver")

            (userProfile, (lastLoginDate, ver))
        })
          .filter(row=>row._2._1 >= dtLastLoginDate)
          .filter(row=>row._2._2 != "3.7.5" || row._1.country != "VN")
          .filter(row=>row._1.country == "VN" || row._1.country == "ID" || row._1.country == "IN")
          .filter(row=>row._1.mbl.length >= 2)
          .map(row=>row._1)
        rddTotalUser
    }


    /**
     * 读取全量用户，并按照沉默天数过滤
     * @param sparkSession Spark 运行环境
     * @param maxSilentDays 最大沉默天数
     * @return DataFrame
     */
    def readTotalUsersFromHive(sparkSession: SparkSession, maxSilentDays: Int = 90): RDD[UserProfile] = {
        val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val dateToday = Calendar.getInstance()
        val dateBefore90 = Calendar.getInstance()
        dateBefore90.add(Calendar.DATE, -maxSilentDays)
        val dtLastLoginDate = timeFormat.format(dateBefore90.getTime)
        var dfUserProfile: DataFrame = null
        do {
            dateToday.add(Calendar.DATE, -1)
            val dtUserDetail = timeFormat.format(dateToday.getTime)
            val sqlText: String = "SELECT DISTINCT hdid, age, sex, country_code, sjp, sjm, mbos, sdkver, mbl, reg_date, last_login_date, nick, uid " +
              " FROM kaixindou.hago_dwd_user_uid_total_detail" +
              s" WHERE dt='${dtUserDetail}' AND country_code in ('VN', 'ID', 'IN') AND (ver!='3.7.5' OR country_code!='VN') AND last_login_date>='${dtLastLoginDate}'"
            dfUserProfile = sparkSession.sql(sqlText)
        } while (dfUserProfile.head(1).isEmpty)
        dfUserProfile.rdd.map(row => {
            try {
                val userProfile: UserProfile = new UserProfile()
                userProfile.devId = row.getString(0)
                userProfile.country = row.getString(3)
                val dateFormat = new SimpleDateFormat("yyyyMMdd")
                userProfile.createTime = dateFormat.parse(row.getString(9))
                userProfile.updateTime = dateFormat.parse(row.getString(10))
                userProfile.os = row.getString(6)
                userProfile.age = row.getInt(1)
                userProfile.sex = row.getInt(2)
                userProfile.nickname = row.getString(11)
                userProfile.mbl = row.getString(8)
                userProfile.uid = row.getLong(12).toString
                userProfile
            } catch {
                case ex: Exception => {
                    println(s"[queryTotalUsers] error message = ${ex.getMessage}")
                    null
                }
            }
        }).filter(r => r!=null)
    }

    /**
     * hive sql 测试
     * @param sc
     * @param path
     * @param maxSilentDays
     * @return
     */
    def testSql(sc: SparkContext, path: String, maxSilentDays: Int = 90):RDD[String] = {
        val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val dateToday = Calendar.getInstance()
        val dateBefore90 = Calendar.getInstance()
        dateBefore90.add(Calendar.DATE, -maxSilentDays)
        val dtLastLoginDate = timeFormat.format(dateBefore90.getTime)
        val dtUserDetail = timeFormat.format(dateToday.getTime)

        val sqlText: String = "SELECT DISTINCT hdid, age, sex, country_code, sjp, sjm, mbos, sdkver, mbl, reg_date, last_login_date, nick, uid " +
          " FROM kaixindou.hago_dwd_user_uid_total_detail" +
          s" WHERE dt='${dtUserDetail}' AND country_code in ('VN', 'ID', 'IN') AND (ver!='3.7.5' OR country_code!='VN') AND last_login_date>='${dtLastLoginDate}'"

        val rddResult: RDD[String] = sc.textFile(path).map( row=> {
            sqlText
        })
        rddResult
    }

    /**
     * ctr 预估数据 join 用户信息数据, join key = hdid
     * @param rawCtrData
     * @param userTotalProfile
     * @return
     */
    def ctrDataJoinUserData(rawCtrData: RDD[RawCtrObj], userTotalProfile: RDD[UserProfile]): RDD[CtrStruct] = {

        val rddTmp = rawCtrData.groupBy(u=>u.hdid).join(userTotalProfile.groupBy(u=>u.devId))
        val rddCtrStruct: RDD[CtrStruct] =
        rddTmp.flatMap( p=>{
            val hdid = p._1
            val ctrJsonArray = JSON.parseArray(p._2._1.head.pctr_context)
            val userInfo = p._2._2.toList.head
            val ctrStructDataArrayBuf: ArrayBuffer[CtrStruct] = ArrayBuffer()

            for( idx <- 0 until ctrJsonArray.size()) {
                val json = ctrJsonArray.getJSONObject(idx)
                val ctrStruct: CtrStruct = new CtrStruct

                ctrStruct.hdid = hdid
                ctrStruct.friend_uid = json.getLong("friend_uid")
                ctrStruct.game_id = json.getString("game_id")
                ctrStruct.pctr = json.getDouble("pctr")
                ctrStruct.country_code = userInfo.country
                ctrStruct.mbl = userInfo.mbl
                ctrStruct.nick = userInfo.nickname

                ctrStructDataArrayBuf.append(ctrStruct)
            }
            ctrStructDataArrayBuf
        })
        rddCtrStruct
    }

    /**
     * 从hdfs读取文案
     * @param sparkContext
     * @param path
     * @return
     */
    def readMessageObjFromHdfs(sparkContext: SparkContext, path:String): RDD[MessageObj] = {
        var rddMessageObj: RDD[MessageObj] = null

        rddMessageObj = sparkContext.textFile(path).map(r=>{
            val messageObj: MessageObj = new MessageObj()
            val parts = r.split("\u0001", -1)
            if(parts.size != 29) {
                null
            } else {
                messageObj.docId = parts(2)
                messageObj.method = parts(3)
                val platformArray = JSON.parseArray(parts(4), classOf[Int])
                var platforms : Array[Int] = Array()
                for(p <- platformArray.toArray()) {
                    platforms = platforms.+:(p.asInstanceOf[Int])
                }
                messageObj.platforms = platforms

                val countryArray = JSON.parseArray(parts(5), classOf[String])
                var country: Array[String] = Array()
                for( c <- countryArray.toArray) {
                    country = country.+:(c.asInstanceOf[String])
                }
                messageObj.country = country

                messageObj.title = parts(14)
                messageObj.content = parts(15)
                messageObj.lang = parts(20)

                messageObj
            }
        }).filter( r=>r!=null)
        rddMessageObj
    }

    /**
     * 从hive表读取文案
     * @param sparkSession
     * @return
     */
    def readMessageObj(sparkSession: SparkSession): RDD[MessageObj] = {
        var rddMessageObj: RDD[MessageObj] = null
        var deltaDays = -1
        do {
            val dtMessageLib : String = TimeUtils.getDateString("yyyyMMdd", deltaDays)
            deltaDays -= 1
            val sql:String =
                s"""SELECT doc_id, method, platforms, country, title, content, lang
                   | FROM kaixindou.hago_push_doc_templates
                   | WHERE dt=${dtMessageLib} AND appid="hago"
                   |""".stripMargin

            rddMessageObj = sparkSession.sql(sql).rdd.map( row => {
                try {
                    val messageObj: MessageObj = new MessageObj()
                    messageObj.docId = row.getString(0)
                    messageObj.method = row.getString(1)

                    val platformArray = JSON.parseArray(row.getString(2), classOf[Int])
                    var platforms : Array[Int] = Array()
                    for(p <- platformArray.toArray()) {
                        platforms = platforms.+:(p.asInstanceOf[Int])
                    }
                    messageObj.platforms = platforms

                    val countryArray = JSON.parseArray(row.getString(3), classOf[String])
                    var country: Array[String] = Array()
                    for( c <- countryArray.toArray) {
                        country = country.+:(c.asInstanceOf[String])
                    }

                    messageObj.country = country
                    messageObj.title = row.getString(4)
                    messageObj.content = row.getString(5)
                    messageObj.lang = row.getString(6)
                    messageObj
                }catch {
                    case _: Throwable => null
                }
            }).filter(m => m != null)
        } while (rddMessageObj.isEmpty() && deltaDays >= -5)

        rddMessageObj
    }


    /**
     * 筛选出来最近14天内给hdid 推荐过的friend_uid，做来过滤 , 这个解析jsonarray是对的
     * @param sc     spark环境
     * @param path   14天推荐输入路径
     * @return
     */
    def readRecommondFriendUid(sc :SparkContext, path: String): RDD[SelfFriendUid] = {
        val rddRec = sc.textFile(path).flatMap( row => {
            val parts = row.split("\t", -1)
            val hdid = parts(0)
            val pctr_context = parts(1)
            val ctrJsonArray = JSON.parseArray(pctr_context)

            val dataBuf: ArrayBuffer[SelfFriendUid] = ArrayBuffer()
            for( idx <- 0 until ctrJsonArray.size()) {
                val selfFriend: SelfFriendUid = new SelfFriendUid()
                val json = ctrJsonArray.getJSONObject(idx)

                selfFriend.hdid = hdid
                selfFriend.friend_uid = json.getLong("friend_uid").toString

                dataBuf.append(selfFriend)
            }

            dataBuf
        })
          .distinct()  // 去掉重复的推荐数据
        rddRec
    }

    /**
     * 读取前14天已经给用过推荐过的好友,用来做过滤
     * @param sc
     * @param path
     * @return
     */
    def readRecommondFriendUidV2(sc :SparkContext, path: String): RDD[SelfFriendUid] = {
        val rddRec = sc.textFile(path).map( row => {
            val parts = row.split("\u0001", -1)
            if (parts.size != 28) {
                null
            } else {
                val hdid = parts(3)
                val cover_id = parts(25)
                val push_type = parts(14)

                if (push_type == "game") {
                    val tmp = cover_id.split(":", -1)
                    if (tmp.size != 2) {
                        null
                    } else {
                        val friend_uid = tmp(1)
                        val selfFriend: SelfFriendUid = new SelfFriendUid()
                        selfFriend.hdid = hdid
                        selfFriend.friend_uid = friend_uid
                        selfFriend
                    }
                } else {
                    null
                }
            }
        })
          .filter(r=>r!=null)
          .distinct()  // 去掉重复的推荐数据
        rddRec
    }

    /**
     * 过滤推荐过的用户
     * @param rddCurRecom
     * @param rddHaveRecom
     * @return
     */
    def filterOut(rddCurRecom: RDD[CtrStruct], rddHaveRecom: RDD[SelfFriendUid]) : RDD[CtrStruct] = {
        var rddResult: RDD[CtrStruct] = null
        rddResult = rddCurRecom.groupBy(u=>u.hdid+u.friend_uid).leftOuterJoin(rddHaveRecom.groupBy(u=>u.hdid+u.friend_uid))
            .map( row => {
                if(row._2._2.isEmpty || row._2._2 == null) {  // 如果没有join上,证明之前没有推荐过friend_uid
                    row._2._1.toList.head
                } else {
                    null
                }
            }).filter( row => row != null)

        rddResult
    }

    /**
     * 过滤掉最近2周给用户推荐过的freind_uid，并根据pctr取前N个做推荐
     * @param rddCurRecom
     * @param rddHaveRecom
     * @param topN
     * @return
     */
    def filterOutAndSelectTopN(rddCurRecom: RDD[CtrStruct], rddHaveRecom: RDD[SelfFriendUid], topN: Int) : RDD[CtrStruct] = {
        var rddResult: RDD[CtrStruct] = null
        rddResult = rddCurRecom.groupBy(u=>u.hdid+u.friend_uid).leftOuterJoin(rddHaveRecom.groupBy(u=>u.hdid+u.friend_uid))
          .map( row => {
              if(row._2._2.isEmpty || row._2._2 == null) {  // 如果没有join上,证明之前没有推荐过friend_uid
                  row._2._1.toList.head
              } else {
                  null
              }
          }).filter( row => row != null)
            .map(r=>{(r.hdid,r)})
            .groupBy(r=>r._1)
            .filter(r=>r._2.nonEmpty)
            .flatMap(r=>{
                val value_list_reverse = r._2.toList.sortBy(_._2.pctr).reverse
                var sz = value_list_reverse.size
                if(sz >= topN) {
                    sz = topN
                }
                val result = for (idx <- 0.until(sz)) yield {
                    value_list_reverse(idx)._2
                }
                result
            })
        rddResult
    }

    /**
     * 读取游戏画像数据
     * @param sparkSession
     * @return
     */
    def readGameInfoFromTable(sparkSession: SparkSession):RDD[(String, String)] = {
        val sess = SparkSession.builder().enableHiveSupport()
          .config("spark.hadoop.validateOutputSpecs", value = true)
          .getOrCreate()

        var rddResult: RDD[(String, String)] = null
        val deltaDays = -1
        val dtGameInfo: String = TimeUtils.getDateString("yyyyMMdd", deltaDays)
        val sqlText: String = "SELECT game_id, country_code, lang, MAX(game_name) as game_name, MAX(game_mode) as game_mode, MAX(game_type) as game_type  FROM kaixindou.hago_push_game_meta_info" +
          s" WHERE dt='${dtGameInfo}' GROUP BY game_id, country_code, lang"

        rddResult = sess.sql(sqlText).rdd.map( row=>{

            (row.getString(0) , row.getInt(5).toString)
        })

        rddResult
    }

    def readGameInfoFromHdfs(sc: SparkContext, path: String):RDD[(String, String)] = {
        var rddResult: RDD[(String, String)] = null
          rddResult = sc.textFile(path).map(r=>{
            val parts = r.split(",", -1)
            if(parts.size != 10){
                null
            } else {
                val game_id = parts(0)
                val game_type = parts(3)  //目前只有一个值: 9
                (game_id, game_type)
            }
        })
          .filter(r=>r!=null)
          .distinct()
        rddResult
    }

//    /**
//     *
//     * @param rddRawCtrData
//     * @param rddHaveRecomFriend
//     * @return
//     */
//    def filterOutRecentRecomFriend(rddRawCtrData:RDD[RawCtrObj], rddHaveRecomFriend:RDD[SelfFriendUid]):RDD[RawCtrObj] = {
//
//    }

    class CtrStruct {
        // 召回逻辑
        var hdid: String = ""
        var friend_uid: Long = 0
        var game_id: String = ""
        var recall_type: String = ""
        var recall_score: Double = 0.0
        var push_friend_cover: String = ""
        var nick: String = ""
        var country_code: String = ""
        var mbl: String = ""
        var friend_nick: String = ""
        var friend_country_code: String = ""
        var friend_mbl: String = ""
        var game_name: String = ""
        var game_mode: String = ""
        var game_type: String = ""
        var game_country_code: String = ""
        var game_lang: String = ""
        var is_loss: Int = 0
        // 拼接文案
        var doc_id: String = ""
        var doc_title: String = ""
        var doc_content: String = ""
        var doc_platforms: Array[Int] = Array()
        var doc_country: Array[String] = Array()
        var doc_lang: String = ""
        var pctr: Double = 0.0
        // 占位符的内容
        var placeholders: Map[String, String] = Map()

        def outputJson(): JSONObject = {
            val jsonDoc: JSONObject = new JSONObject()
            jsonDoc.put("hdid", hdid)
            jsonDoc.put("country", country_code)
            jsonDoc.put("docId", doc_id)
            jsonDoc.put("resId", game_id)
            jsonDoc.put("title", doc_title)
            jsonDoc.put("content", doc_content)
            jsonDoc.put("coverUrl", push_friend_cover)
            jsonDoc.put("gameId", game_id)
            jsonDoc.put("gameName", game_name)
            jsonDoc.put("gameType", game_type)
            jsonDoc.put("resType", "game")
            jsonDoc.put("coverId", s"uid:${friend_uid}")
            jsonDoc.put("pctr", pctr)
            if (is_loss > 0) {
                jsonDoc.put("style", "popup")
            } else {
                jsonDoc.put("style", "notify")
            }

            jsonDoc
        }

        def assemble(message: MessageObj): Unit = {
            doc_id = message.docId
            doc_title = message.title
            doc_content = message.content
            doc_platforms = message.platforms
            doc_country = message.country
            doc_lang = message.lang
        }

        def isDocValid(): Boolean = {
            hdid.length > 0 && friend_uid > 0 && game_id.length > 0 && doc_id.length > 0 &&
              doc_title.length > 0 && doc_content.length > 0 && !doc_title.contains('$') && !doc_content.contains('$')
        }

        def isMatched(messageObj: MessageObj): Boolean = {  // 国家、操作系统语言、paltform=3 安卓 匹配
            messageObj.country.contains(country_code) &&
              mbl.substring(0, 2) == messageObj.lang &&
              messageObj.platforms.contains(3)
        }

        def initPlaceholders(): Unit = {
            placeholders = placeholders.+(("fdgame_name", game_name))
            placeholders = placeholders.+(("fdgame_number", (Random.nextInt(30) + 10).toString))
            placeholders = placeholders.+(("fdonlinegame_name", game_name))
            placeholders = placeholders.+(("fdonlinegame_nick", friend_nick))
            placeholders = placeholders.+(("citygame_name", game_name))
            placeholders = placeholders.+(("fdwingame_name", game_name))
            placeholders = placeholders.+(("fdwingame_number", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("fdstreakgame_name", game_name))
            placeholders = placeholders.+(("fdstreakgame_number", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("fdstreakgame_nick", friend_nick))
            placeholders = placeholders.+(("pkstreakgame_name", game_name))
            placeholders = placeholders.+(("pkstreakgame_number", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("similargame_name", game_name))
            placeholders = placeholders.+(("recent_likegame_name", game_name))
            placeholders = placeholders.+(("recent_likegame_name_win", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("recent_likegame_name_lost", (Random.nextInt(5) + 5).toString))
            placeholders = placeholders.+(("recent_likegame_name_play", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("history_likegame_name", game_name))
            placeholders = placeholders.+(("history_likegame_name_win", (Random.nextInt(10) + 10).toString))
            placeholders = placeholders.+(("history_likegame_name_lost", (Random.nextInt(5) + 5).toString))
            placeholders = placeholders.+(("history_likegame_name_play", (Random.nextInt(10) + 10).toString))
        }
    }

    /**
     * ctr预估数据结构
     */
    class RawCtrObj {
        var hdid :String = ""
        var pctr_context: String = ""
    }

    class SelfFriendUid {
        var hdid: String = ""
        var friend_uid: String = ""
    }

    class CtrJson {
        var friend_uid: String = ""
        var game_id: String = ""
        var pctr: Double = 0.0
    }

    class GameInfo {
        var game_id: String = ""
        var game_type: Int = 9
    }
}
