package poly.service.impl.analysis.rank.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;

import poly.dto.redis.RankResultDTO;
import poly.persistance.redis.ICacheRedisMapper;
import poly.service.impl.analysis.rank.ILikeAnalysisService;
import poly.service.impl.analysis.rank.IPrgAnalysisService;
import poly.service.impl.analysis.rank.IQuizRankService;
import poly.service.impl.analysis.rank.IReadAnalysisService;
import poly.service.impl.analysis.rank.IReadIcrAnalysisService;
import poly.service.impl.analysis.rank.IShareAnalysisService;
import poly.service.impl.analysis.rank.ITrueRateAnalysisService;
import poly.service.impl.analysis.rank.IZzimAnalysisService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.util.CmmUtil;
import poly.util.DateUtil;
import poly.util.NumberUtil;

@Service("QuizRankService")
public class QuizRankService extends MongoDBComon implements IQuizRankService, ICommCont {

	/*
	 * #############################################################################
	 * 정의된 상수
	 * #############################################################################
	 */
	// 인기퀴즈 제외 단어
	List<String> stopWords = null;


	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	// 퀴즈 정답률 분석 서비스 객체 생성
	@Resource(name = "TrueRateAnalysisService")
	private ITrueRateAnalysisService trueRateAnalysisService;

	// 퀴즈 진행률 분석 서비스 객체 생성
	@Resource(name = "PrgAnalysisService")
	private IPrgAnalysisService prgAnalysisService;

	// 퀴즈 좋아요수 분석 서비스 객체 생성
	@Resource(name = "LikeAnalysisService")
	private ILikeAnalysisService likeAnalysisService;

	// 퀴즈 조회수 분석 서비스 객체 생성
	@Resource(name = "ReadAnalysisService")
	private IReadAnalysisService readAnalysisService;

	// 퀴즈 공유하기 분석 서비스 객체 생성
	@Resource(name = "ShareAnalysisService")
	private IShareAnalysisService shareAnalysisService;

	// 찜 분석 서비스 객체 생성
	@Resource(name = "ZzimAnalysisService")
	private IZzimAnalysisService zzimAnalysisService;

	// 최근 조회 증가율 분석 서비스 객체 생성
	@Resource(name = "ReadIcrAnalysisService")
	private IReadIcrAnalysisService readIcrAnalysisService;

	// 인기 퀴즈 분석 최종 결과를 Redis에 저장
	@Resource(name = "CacheRedisMapper")
	private ICacheRedisMapper cacheRedisMapper;

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 퀴즈팩 태그 및 분석단어가 제외단어가 포함되었는지 체크
	 */
	private boolean doCheckWord(String word) throws Exception {
		// 인기퀴즈 전용 제외 키워드
		String[] stopWord = IQuizRankService.stopWord;
		int wordCnt = stopWord.length;

		boolean res = false;

		for (int s = 0; s < wordCnt; s++) {
			if (word.indexOf(stopWord[s]) > -1) {
				res = true;
				break;
			}
		}

		return res;
	}

	/**
	 * 분석 제외 단어 가져오기
	 * 
	 */
	private int doStopWord() {

		log.info(this.getClass().getName() + ".doStopWord Start!");

		int res = 0;

		// 기존데이터 삭제하기
		this.stopWords = null;

		// 분석 제외 단어 메모리에 올리기
		this.stopWords = new LinkedList<String>();

		String colNm = "QUIZRANK_STOP_WORD";

		// 컬렉션 정보가져오기
		MongoCollection<Document> col = mongodb.getCollection(colNm);

		Document projection = new Document();
		projection.append("word", "$word");
		projection.append("_id", 0);

		// 컬렉션 정보가져오기
		FindIterable<Document> rs = col.find(new Document()).projection(projection);
		Iterator<Document> cursor = rs.iterator();

		while (cursor.hasNext()) {
			Document doc = cursor.next();

			if (doc == null) {
				doc = new Document();
			}

			String word = CmmUtil.nvl(doc.getString("word"));
			log.info("word : " + word);

			this.stopWords.add(word);

			doc = null;
		}

		projection = null;
		cursor = null;
		rs = null;
		col = null;

		res = 1;

		log.info(this.getClass().getName() + ".doStopWord End!");

		return res;
	}

	/**
	 * 인기퀴즈 구현을 위한 각 도메인별 최종 데이터 분석 1단계
	 */
	private int doDataAnalysisStep1() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysisStep1 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		// 일괄저장을 위한 객체
		List<Document> sList = null;

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 생성 시작!
		 * #############################################################################
		 */

		// 각 분석 도메인별 데이터 병합할 컬렉션 이름
		String nColNm = "QUIZRANK_ANALYSIS_" + ContAnaysisStdDay + "_STEP1";

		// 기존에 등록된 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 삭제
		if (mongodb.collectionExists(nColNm)) {
			mongodb.dropCollection(nColNm);

		}

		// 각 분석 도메인별 데이터 병합할 컬렉션 생성
		mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 생성 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 시작!
		 * #############################################################################
		 */

		MongoCollection<Document> rCol = null;
		Iterator<Document> cursor = null;
		FindIterable<Document> rs = null;

		Document doc = null;

		// 좋아요 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_LIKE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("likeRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("likeRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 조회수 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_READ_ANALYSIS_" + ContAnaysisStdDay + "_STEP4");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("readRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("readRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			MongoCollection<Document> col = mongodb.getCollection(nColNm);
			col.insertOne(new Document(doc));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 찜 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_ZZIM_ANALYSIS_" + ContAnaysisStdDay + "_STEP4");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("zzimRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("zzimRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 공유하기 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("shareRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("shareRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 정답률 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP5");

		Document query = new Document();

		// 정답률 0.86%이하
		query.append("stdRate", new Document().append("$lt", this.stdQuizPackTrueRate));

		Document projection = new Document();

		projection.append("qp_id", "$qp_id");
		projection.append("stdRate", "$stdRate");
		projection.append("rateWeight", "$rateWeight");
		projection.append("_id", 0);

		rs = rCol.find(query).projection(projection);

		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("trueRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("trueRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 진행률 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_PRG_ANALYSIS_" + ContAnaysisStdDay + "_STEP5");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("prgRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("prgRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		// 최근 조회수 증가율 컬렉션
		rCol = mongodb.getCollection("QUIZRANK_READ_ICR_ANALYSIS_" + ContAnaysisStdDay + "_STEP3");

		rs = rCol.find();
		cursor = rs.iterator();

		sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));
			doc.append("readIcrRate", NumberUtil.getDouble(current.get("stdRate")));
			doc.append("readIcrRateWeight", NumberUtil.getDouble(current.get("rateWeight")));

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;
		rCol = null;

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 끝!
		 * #############################################################################
		 */

		log.info("#################################################################");
		log.info("# doDataAnalysisStep1 Result!!");
		log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
		log.info("#################################################################");

		log.info(this.getClass().getName() + ".doDataAnalysisStep1 End!");

		return 1;
	}

	/**
	 * 인기퀴즈 구현을 위한 각 도메인별 최종 데이터 분석 2단계
	 */
	private int doDataAnalysisStep2() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysisStep2 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		/*
		 * #############################################################################
		 * 빅데이터분석 최종 결과 저장 컬렉셩 생성 시작!
		 * #############################################################################
		 */

		// 빅데이터분석 최종 결과 저장할 컬렉션 이름
		String nColNm = "QUIZRANK_ANALYSIS_" + ContAnaysisStdDay + "_STEP2";

		super.DeleteCreateCollectionUniqueIndex(nColNm, "qp_id");
		
		/*
		 * #############################################################################
		 * 빅데이터분석 최종 결과 저장 컬렉셩 생성 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 시작!
		 * #############################################################################
		 */

		MongoCollection<Document> rCol = mongodb.getCollection("QUIZRANK_ANALYSIS_" + ContAnaysisStdDay + "_STEP1");

		// 데이터분석 쿼리 생성하기
		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$group",
						new Document().append("_id", new Document().append("qp_id", "$qp_id"))
								.append("SUM(trueRate)", new Document().append("$sum", "$trueRate"))
								.append("SUM(trueRateWeight)", new Document().append("$sum", "$trueRateWeight"))
								.append("SUM(prgRate)", new Document().append("$sum", "$prgRate"))
								.append("SUM(prgRateWeight)", new Document().append("$sum", "$prgRateWeight"))
								.append("SUM(readRate)", new Document().append("$sum", "$readRate"))
								.append("SUM(readRateWeight)", new Document().append("$sum", "$readRateWeight"))
								.append("SUM(likeRate)", new Document().append("$sum", "$likeRate"))
								.append("SUM(likeRateWeight)", new Document().append("$sum", "$likeRateWeight"))
								.append("SUM(zzimRate)", new Document().append("$sum", "$zzimRate"))
								.append("SUM(zzimRateWeight)", new Document().append("$sum", "$zzimRateWeight"))
								.append("SUM(shareRate)", new Document().append("$sum", "$shareRate"))
								.append("SUM(shareRateWeight)", new Document().append("$sum", "$shareRateWeight"))
								.append("SUM(readIcrRate)", new Document().append("$sum", "$readIcrRate"))
								.append("SUM(readIcrRateWeight)", new Document().append("$sum", "$readIcrRateWeight"))),
				new Document().append("$project",
						new Document().append("qp_id", "$_id.qp_id").append("trueRate", "$SUM(trueRate)")
								.append("trueRateWeight", "$SUM(trueRateWeight)").append("prgRate", "$SUM(prgRate)")
								.append("prgRateWeight", "$SUM(prgRateWeight)").append("readRate", "$SUM(readRate)")
								.append("readRateWeight", "$SUM(readRateWeight)").append("likeRate", "$SUM(likeRate)")
								.append("likeRateWeight", "$SUM(likeRateWeight)").append("zzimRate", "$SUM(zzimRate)")
								.append("zzimRateWeight", "$SUM(zzimRateWeight)").append("shareRate", "$SUM(shareRate)")
								.append("shareRateWeight", "$SUM(shareRateWeight)")
								.append("readIcrRate", "$SUM(readIcrRate)")
								.append("readIcrRateWeight", "$SUM(readIcrRateWeight)").append("_id", 0)),
				new Document().append("$match", new Document().append("trueRate", new Document().append("$gt", 0L))));

		AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
		Iterator<Document> cursor = rs.iterator();

		Document doc = null;

		// 일괄저장을 위한 객체 생성
		List<Document> sList = new ArrayList<Document>();

		// 퀴즈팩별 좋아요율 일자별 저장하기
		while (cursor.hasNext()) {

			final Document current = cursor.next();

			doc = new Document();

			doc.append("stdDay", ContAnaysisStdDay);
			doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));

			// 조회수
			final double readRate = NumberUtil.getDouble(current.get("readRate"));
			final double readRateWeight = NumberUtil.getDouble(current.get("readRateWeight"));

			doc.append("readRate", readRate);
			doc.append("readRateWeight", readRateWeight);

			// 좋아요 분석
			final double likeRate = NumberUtil.getDouble(current.get("likeRate"));
			final double likeRateWeight = NumberUtil.getDouble(current.get("likeRateWeight"));

			doc.append("likeRate", likeRate);
			doc.append("likeRateWeight", likeRateWeight);

			// 찜 분석
			final double zzimRate = NumberUtil.getDouble(current.get("zzimRate"));
			final double zzimRateWeight = NumberUtil.getDouble(current.get("zzimRateWeight"));

			doc.append("zzimRate", zzimRate);
			doc.append("zzimRateWeight", zzimRateWeight);

			// 공유하기 분석
			final double shareRate = NumberUtil.getDouble(current.get("shareRate"));
			final double shareRateWeight = NumberUtil.getDouble(current.get("shareRateWeight"));

			doc.append("shareRate", shareRate);
			doc.append("shareRateWeight", shareRateWeight);

			// 정답률 분석
			final double trueRate = NumberUtil.getDouble(current.get("trueRate"));
			final double trueRateWeight = NumberUtil.getDouble(current.get("trueRateWeight"));

			doc.append("trueRate", trueRate);
			doc.append("trueRateWeight", trueRateWeight);

			// 진행률 분석
			final double prgRate = NumberUtil.getDouble(current.get("prgRate"));
			final double prgRateWeight = NumberUtil.getDouble(current.get("prgRateWeight"));

			doc.append("prgRate", prgRate);
			doc.append("prgRateWeight", prgRateWeight);

			// 최근 조회수 증가율 분석
			final double readIcrRate = NumberUtil.getDouble(current.get("readIcrRate"));
			final double readIcrRateWeight = NumberUtil.getDouble(current.get("readIcrRateWeight"));

			doc.append("readIcrRate", readIcrRate);
			doc.append("readIcrRateWeight", readIcrRateWeight);

			// 기존 항목(진행률, 조회율, 정답률) 결과값 저장
			double sumRateWeight1 = (readRateWeight * this.stdReadRate) + (prgRateWeight * this.stdPrgRate)
					+ (trueRateWeight * this.stdTrueRate) + likeRateWeight + zzimRateWeight + shareRateWeight;

			// 기존항목 가중치
			final double rateWeight1 = Math.round((sumRateWeight1 * stdTotalRate) / ContReadCntDay * ContDataSize)
					/ ContDataSize;

			// 최근 조회수 증가율
			double sumRateWeight2 = readIcrRateWeight * this.stdReadIcrRate;

			// 최근 조회수 증가율 가중치
			final double rateWeight2 = Math.round(sumRateWeight2 / ContReadCntDay * ContDataSize) / ContDataSize;

			doc.append("rateWeight", rateWeight1 + rateWeight2);

			sList.add(doc); // 저장할 데이터 저장

			doc = null;

		}

		// 분할 일괄저장하기
		super.insertMany(nColNm, sList, 10000);

		sList = null;

		cursor = null;
		rs = null;

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 끝!
		 * #############################################################################
		 */

		log.info("#################################################################");
		log.info("# doDataAnalysisStep2 Result!!");
		log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
		log.info("#################################################################");

		log.info(this.getClass().getName() + ".doDataAnalysisStep2 End!");

		return 1;
	}

	/**
	 * 인기퀴즈 구현을 위한 각 도메인별 최종 데이터 분석 3단계
	 */
	private int doDataAnalysisStep3() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysisStep3 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		/*
		 * #############################################################################
		 * 빅데이터분석 최종 결과 저장 컬렉셩 생성 시작!
		 * #############################################################################
		 */

		// 빅데이터분석 최종 결과 저장할 컬렉션 이름
		String nColNm = "QUIZRANK_ANALYSIS_" + ContAnaysisStdDay + "_STEP3";

		super.DeleteCreateCollectionUniqueIndex(nColNm, "qp_id");

		/*
		 * #############################################################################
		 * 빅데이터분석 최종 결과 저장 컬렉셩 생성 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 시작!
		 * #############################################################################
		 */

		String stdColNm = "QUIZRANK_ANALYSIS_" + ContAnaysisStdDay + "_STEP2"; // 기준 컬렌션
		String joinColNm = "NLP_QUIZPACK_DICTIONARY"; // 조인 대상 컬렉션

		MongoCollection<Document> rCol = mongodb.getCollection(stdColNm);

		// 데이터분석 쿼리 생성하기
		List<? extends Bson> pipeline = Arrays
				.asList(new Document().append("$project", new Document().append("_id", 0).append(stdColNm, "$$ROOT")),
						new Document().append("$lookup",
								new Document().append("localField", stdColNm + ".qp_id").append("from", joinColNm)
										.append("foreignField", "qp_id").append("as", joinColNm)),
						new Document().append("$unwind", new Document()
								.append("path", "$" + joinColNm).append("preserveNullAndEmptyArrays", false)),
						new Document()
								.append("$match",
										new Document().append(joinColNm + ".nn", new Document().append("$not",
												new Document().append("$in", this.stopWords)))),
						new Document().append("$sort", new Document().append(stdColNm + ".rateWeight", -1)),
						new Document().append("$project",
								new Document().append("qp_id", "$" + stdColNm + ".qp_id")
										.append("subject", "$" + joinColNm + ".subject")
										.append("tags", "$" + joinColNm + ".tags").append("nn", "$" + joinColNm + ".nn")
										.append("stdDay", "$" + stdColNm + ".stdDay")
										.append("readRate", "$" + stdColNm + ".readRate")
										.append("readRateWeight", "$" + stdColNm + ".readRateWeight")
										.append("likeRate", "$" + stdColNm + ".likeRate")
										.append("likeRateWeight", "$" + stdColNm + ".likeRateWeight")
										.append("zzimRate", "$" + stdColNm + ".zzimRate")
										.append("zzimRateWeight", "$" + stdColNm + ".zzimRateWeight")
										.append("shareRate", "$" + stdColNm + ".shareRate")
										.append("shareRateWeight", "$" + stdColNm + ".shareRateWeight")
										.append("trueRate", "$" + stdColNm + ".trueRate")
										.append("trueRateWeight", "$" + stdColNm + ".trueRateWeight")
										.append("prgRate", "$" + stdColNm + ".prgRate")
										.append("prgRateWeight", "$" + stdColNm + ".prgRateWeight")
										.append("readIcrRate", "$" + stdColNm + ".readIcrRate")
										.append("readIcrRateWeight", "$" + stdColNm + ".readIcrRateWeight")
										.append("rateWeight", "$" + stdColNm + ".rateWeight").append("_id", 0)));

		AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
		Iterator<Document> cursor = rs.iterator();

		// 기존 등록된 분석 결과(Redis) 삭제
		cacheRedisMapper.deleteRankResult();

		int i = 0;

		Document doc = null;

		// MongoDB에 일괄 데이터 저장을 위한 객체
		List<Document> sList = new ArrayList<Document>();

		while (cursor.hasNext()) {

			final Document current = cursor.next();

			i++;

			// MongoDB 저장용 객체
			doc = new Document();

			// 퀴즈팩 아이디
			String qp_id = CmmUtil.nvl(current.getString("qp_id"));
			doc.append("qp_id", qp_id);

			// 퀴즈팩 이름
			String qp_nm = CmmUtil.nvl(current.getString("subject"));
			doc.append("qp_nm", qp_nm);

			// 퀴즈팩 태그
			doc.append("qp_tag", CmmUtil.nvl(current.getString("tags")));

			doc.append("nn", current.getList("nn", String.class, new ArrayList<String>()));

			// 인기 순위
			doc.append("qp_rank", String.valueOf(i));

			// 분석일자
			doc.append("stdDay", ContAnaysisStdDay);

			// 조회수
			final double readRate = NumberUtil.getDouble(current.get("readRate"));
			final double readRateWeight = NumberUtil.getDouble(current.get("readRateWeight"));

			doc.append("readRate", readRate);
			doc.append("readRateWeight", readRateWeight);

			// 좋아요 분석
			final double likeRate = NumberUtil.getDouble(current.get("likeRate"));
			final double likeRateWeight = NumberUtil.getDouble(current.get("likeRateWeight"));

			doc.append("likeRate", likeRate);
			doc.append("likeRateWeight", likeRateWeight);

			// 찜 분석
			final double zzimRate = NumberUtil.getDouble(current.get("zzimRate"));
			final double zzimRateWeight = NumberUtil.getDouble(current.get("zzimRateWeight"));

			doc.append("zzimRate", zzimRate);
			doc.append("zzimRateWeight", zzimRateWeight);

			// 공유하기 분석
			final double shareRate = NumberUtil.getDouble(current.get("shareRate"));
			final double shareRateWeight = NumberUtil.getDouble(current.get("shareRateWeight"));

			doc.append("shareRate", shareRate);
			doc.append("shareRateWeight", shareRateWeight);

			// 정답률 분석
			final double trueRate = NumberUtil.getDouble(current.get("trueRate"));
			final double trueRateWeight = NumberUtil.getDouble(current.get("trueRateWeight"));

			doc.append("trueRate", trueRate);
			doc.append("trueRateWeight", trueRateWeight);

			// 진행률 분석
			final double prgRate = NumberUtil.getDouble(current.get("prgRate"));
			final double prgRateWeight = NumberUtil.getDouble(current.get("prgRateWeight"));

			doc.append("prgRate", prgRate);
			doc.append("prgRateWeight", prgRateWeight);

			// 최근 조회수 증가율 분석
			final double readIcrRate = NumberUtil.getDouble(current.get("readIcrRate"));
			final double readIcrRateWeight = NumberUtil.getDouble(current.get("readIcrRateWeight"));

			doc.append("readIcrRate", readIcrRate);
			doc.append("readIcrRateWeight", readIcrRateWeight);

			// 최종 결과값 저장
			final double rateWeight = NumberUtil.getDouble(current.get("rateWeight"));

			doc.append("rateWeight", rateWeight);

			// MongoDB 일괄 저장을 위해 데이터 넣기
			sList.add(doc);

			doc = null;

			// 500 데이터까지만 저장함
			if (i < (ICommCont.ContQuizRank)) {

				// Redis에 저장할 데이터만 추가
				RankResultDTO pDTO = new RankResultDTO();

				pDTO.setQp_id(qp_id);
				pDTO.setQp_nm(qp_nm);
				pDTO.setQp_rank(i);
				pDTO.setRate_weight(rateWeight);

				cacheRedisMapper.insertRankResult(pDTO, i);

				pDTO = null;
			}

		}

		// 몽고DB에 일괄 저장
		MongoCollection<Document> col = mongodb.getCollection(nColNm);
		col.insertMany(sList);
		col = null;

		sList = null;

		// Redis TTL 지정
		cacheRedisMapper.setQuizRankExpire();
		/*
		 * #############################################################################
		 * 각 분석 도메인별 데이터 병합할 컬렉션 데이터 구조 생성 끝!
		 * #############################################################################
		 */

		log.info("#################################################################");
		log.info("# doDataAnalysisStep3 Result!!");
		log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
		log.info("#################################################################");

		log.info(this.getClass().getName() + ".doDataAnalysisStep3 End!");

		return 1;
	}

	/**
	 * 이전 날짜의 사용하지 않는 데이터 삭제
	 */
	private int doCleanData() throws Exception {

		log.info(this.getClass().getName() + ".doCleanData Start!");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			String cleanDay = DateUtil.getDateTimeAdd(-1); // 삭제 날짜(하루 전)
			String dropColNm = "";

			// 1단계 분석 삭제
			dropColNm = "QUIZRANK_ANALYSIS_" + cleanDay + "_STEP1";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 2단계 분석 삭제
			dropColNm = "QUIZRANK_ANALYSIS_" + cleanDay + "_STEP2";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 3단계 분석 삭제
			dropColNm = "QUIZRANK_ANALYSIS_" + cleanDay + "_STEP3";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			result = 1;

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doCleanData Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doCleanData End!");

		return result;

	}

	@Override
	public int doDataAnalysis() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysis Start!");

		int res = 0;

		/*
		 * #############################################################################
		 * 01.퀴즈팩 정답률 분석 시작
		 * #############################################################################
		 */

		if (trueRateAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " trueRateAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 01.퀴즈팩 정답률 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 02.퀴즈팩 진행률 분석 시작
		 * #############################################################################
		 */

		if (prgAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " prgAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 02.퀴즈팩 진행률 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 03.퀴즈팩 조회수 분석 시작
		 * #############################################################################
		 */

		if (readAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " readAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 03.퀴즈팩 조회수 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 04.퀴즈팩 좋아요 분석 시작
		 * #############################################################################
		 */

		if (likeAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " likeAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 04.퀴즈팩 좋아요 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 05.찜 분석 시작
		 * #############################################################################
		 */

		if (zzimAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " zzimAnaysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 05.찜 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 06.공유하기 분석 시작
		 * #############################################################################
		 */

		if (shareAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " shareAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 06.공유하기 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 07.최근 조회수 증가율 분석 시작
		 * #############################################################################
		 */

		if (readIcrAnalysisService.doAnalysis() != 1) {
			log.info(this.getClass().getName() + " readIcrAnalysisService.doAnalysis() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 07.최근 조회수 증가율 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 07.빅데이터 최종 분석 시작
		 * #############################################################################
		 */

		// 인기퀴즈 제외 단어 가져오기
		if (this.doStopWord() != 1) {
			log.info(this.getClass().getName() + " doStopWord() Fail !!");
			return 0;

		}

		if (doDataAnalysisStep1() != 1) {
			log.info(this.getClass().getName() + " doDataAnalysisStep1() Fail !!");
			return 0;

		}

		if (doDataAnalysisStep2() != 1) {
			log.info(this.getClass().getName() + " doDataAnalysisStep2() Fail !!");
			return 0;

		}

		if (doDataAnalysisStep3() != 1) {
			log.info(this.getClass().getName() + " doDataAnalysisStep3() Fail !!");
			return 0;

		}

		/*
		 * #############################################################################
		 * 07.빅데이터 최종 분석 끝
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 데이터 클린징 시작!!
		 * #############################################################################
		 */
		if (doCleanData() != 1) {
			log.info("doCleanData Fail !!");

			return 0;
		}

		res = 1;
		log.info(this.getClass().getName() + ".doDataAnalysisForQuizRank End!");

		return res;
	}

}
