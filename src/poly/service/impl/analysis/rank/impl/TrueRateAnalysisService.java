package poly.service.impl.analysis.rank.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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

import poly.service.impl.analysis.rank.ITrueRateAnalysisService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.util.CmmUtil;
import poly.util.DateUtil;
import poly.util.NumberUtil;

@Service("TrueRateAnalysisService")
public class TrueRateAnalysisService extends MongoDBComon implements ITrueRateAnalysisService, ICommCont {

	/*
	 * #############################################################################
	 * 정의된 상수
	 * #############################################################################
	 */
	// 빅데이터 분석을 위한 조회수 조회기간
	// final private String toDay = DateUtil.getDateTime("yyyyMMdd");

	// 빅데이터 가중치 계산을 위한 연산 자리수(기본값은 소수점 4자리)
	// final private double dataSize = 10000d;

	// 빅데이터 분석을 위한 조회수 조회기간
	// final private int readCntDay = 7;

	private double trueRateForQuizPack = 0;

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 정답률 분석 1단계
	 * 
	 * 퀴즈팩별 정답률 분석 - QUIZ_LOG_STEP2_yyyyMMdd 데이터로부터 분석 - 1차 데이터분석 진행
	 */
	private int doAnalysisStep1() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep1 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션 이름
			String nColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP1";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			// 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션 생성
			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션의 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			// 인기 퀴즈 구현을 위한 로그 최종 정제결과 가져오기
			MongoCollection<Document> rCol = mongodb.getCollection("QUIZRANK_DATA_BN_" + ContAnaysisStdDay + "_STEP3");

			// 데이터 쿼리 생성하기
			// 퀴즈팩 정답률이 0보다 큰 것만 가져오기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document().append("qp_id", "$qp_id"))
									.append("AVG(answerTrueRate)", new Document().append("$avg", "$answerTrueRate"))),
					new Document().append("$project",
							new Document().append("qp_id", "$_id.qp_id").append("AVG_TRUE_RATE", "$AVG(answerTrueRate)")
									.append("_id", 0)),
					new Document().append("$match",
							new Document().append("AVG_TRUE_RATE", new Document().append("$gt", 0L))));
			// 퀴즈팩의 정답률이 0보다 큰것만 가져오도록 쿼리 추가

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			// 퀴즈팩별 좋아요율 일자별 저장하기
			while (cursor.hasNext()) {

				doc = new Document();

				final Document current = cursor.next();

				// 퀴즈팩별 정답률
				final double answerTrueRate = NumberUtil.getDouble(current.get("AVG_TRUE_RATE"));

				// 퀴즈팩
				final String qp_id = CmmUtil.nvl((String) current.get("qp_id"));

				doc.append("qp_id", qp_id);
				doc.append("trueRate", answerTrueRate);

				sList.add(doc); // 저장할 데이터 저장

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션의 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep1 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 퀴즈팩별 정답률 분석 결과 저장을 위한 컬렉션의 데이터 구조 생성 끝 !
			 * #############################################################################
			 */

			result = 1; // 데이터 처리가 성공하면 1로 값 변경

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doAnalysisStep1 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep1 End!");
		return result;
	}

	/**
	 * 정답률 분석 2단계
	 * 
	 * 오늘의 퀴즈팩별 평균 정답률 분석 - 1차 데이터분석 결과 데이터(QUIZ_TRUE_RATE_STEP1_yyyyMMdd)로부터 분석 -
	 * 2차 데이터분석 진행
	 */
	private int doAnalysisStep2() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep2 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 오늘의 퀴즈팩별 평균 정답률 데이터가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP2";

			// 기존에 등록된 오늘의 퀴즈팩별 평균 정답률 데이터 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			// 오늘의 퀴즈팩별 평균 정답률 데이터가 저장되는 컬렉션 생성
			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("true_date"));

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 생성 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP1");

			// 데이터 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document()).append("AVG(trueRate)",
									new Document().append("$avg", "$trueRate"))),
					new Document().append("$project",
							new Document().append("TRUE_RATE", "$AVG(trueRate)").append("_id", 0)));

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			// 퀴즈팩별 좋아요율 일자별 저장하기
			if (cursor.hasNext()) {

				doc = new Document();

				final Document current = cursor.next();

				doc.append("trueDate", ContAnaysisStdDay);

				// 퀴즈팩별 정답률
				final double trueRate = Math.round(NumberUtil.getDouble(current.get("TRUE_RATE")) * ContDataSize)
						/ ContDataSize;

				// 전역변수에 정답률 정의하기
				this.trueRateForQuizPack = trueRate;

				log.info("trueRate : " + trueRate);
				doc.append("trueRate", trueRate);

				sList.add(doc);

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep1 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 데이터 구조 생성 끝 !
			 * #############################################################################
			 */

			result = 1; // 데이터 처리가 성공하면 1로 값 변경

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doAnalysisStep2 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep2 End!");
		return result;
	}

	/**
	 * 정답률 분석 3단계
	 * 
	 * 가중치가 적용된 퀴즈팩별 정답률 데이터 분석 - 3차 데이터분석 결과 데이터(QUIZ_TRUE_RATE_STEP1_yyyyMMdd)로부터
	 * 분석 - 2차 데이터분석 진행
	 */
	private int doAnalysisStep3() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep3 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 가중치가 적용된 퀴즈팩별 정답률 데이터 분석 결과가 저장되는 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 가중치가 적용된 퀴즈팩별 정답률 데이터 분석 결과가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP3";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			// 가중치가 적용된 퀴즈팩별 정답률 데이터 분석 결과가 저장되는 컬렉션 생성
			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 가중치가 적용된 퀴즈팩별 정답률 데이터 분석 결과가 저장되는 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 정답률 가중치 계산값이 저장되는 컬렉션 생성 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			// 퀴즈팩별 정답률 데이터 가져오기
			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP1");

			FindIterable<Document> rs = rCol.find();
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			while (cursor.hasNext()) {

				doc = new Document();

				final Document current = cursor.next();

				// 퀴즈팩 아이디
				String qp_id = CmmUtil.nvl((String) current.get("qp_id"));

				doc.append("qp_id", qp_id);

				// 퀴즈팩별 평균정답률
				double avrTrueRate = NumberUtil.getDouble(current.get("trueRate"));

				doc.append("trueRate", avrTrueRate);

				// 전체 퀴즈팩 정답률과 퀴즈팩별 평균 정답률에 대한 가중치 계산
				double rateWeight = 1
						- Math.round(Math.abs(this.trueRateForQuizPack - avrTrueRate) * ContDataSize) / ContDataSize;

				doc.append("rateWeight", rateWeight);

				sList.add(doc);

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 정답률 가중치 계산값이 저장되는 컬렉션 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep3 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 정답률 가중치 계산값이 저장되는 컬렉션 데이터 구조 생성 끝 !
			 * #############################################################################
			 */

			result = 1; // 데이터 처리가 성공하면 1로 값 변경

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doAnalysisStep3 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep3 End!");
		return result;
	}

	/**
	 * 정답률 분석 4단계
	 * 
	 * 최근 7일동안의 일자별 퀴즈팩들의 정답률 및 가중치 적용된 정답률 데이터 구조 생성
	 */
	private int doAnalysisStep4() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep4 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 정답률 및 가중치 적용된 정답률 데이터 구조를 저장할 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			// 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 생성
			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 정답률 및 가중치 적용된 정답률 데이터 구조를 저장할 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 정답률 및 가중치 적용된 정답률 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			for (int i = 0; i < ContReadCntDay; i++) {

				String stdDay = DateUtil.getDateTimeAdd(-i); // 분석 일자

				// 이전 조회수 저장 컬렉션 로드하기
				MongoCollection<Document> rCol = mongodb
						.getCollection("QUIZRANK_TRUE_RATE_ANALYSIS_" + stdDay + "_STEP3");

				log.info("#################################################################");
				log.info("# Read QUIZRANK_TRUE_RATE_ANALYSIS_" + stdDay + "_STEP3 Analysis Start!!");
				log.info("#################################################################");

				FindIterable<Document> rs = rCol.find();
				Iterator<Document> cursor = rs.iterator();

				Document doc = null;

				// 일괄저장을 위한 객체 생성
				List<Document> sList = new ArrayList<Document>();

				while (cursor.hasNext()) {

					doc = new Document();

					final Document current = cursor.next();

					// 퀴즈팩 아이디
					String qp_id = CmmUtil.nvl((String) current.get("qp_id"));

					doc.append("qp_id", qp_id);

					double trueRate = NumberUtil.getDouble(current.get("trueRate")); // 정답률
					double rateWeight = NumberUtil.getDouble(current.get("rateWeight")); // 가준치가 적용된 정답률

					// 정답률 기준 날짜라면..
					if (i == 0) {
						doc.append("stdRate1", trueRate);
						doc.append("rateWeight1", rateWeight);

					} else if (i == 1) {
						doc.append("stdRate2", trueRate);
						doc.append("rateWeight2", rateWeight);

					} else if (i == 2) {
						doc.append("stdRate3", trueRate);
						doc.append("rateWeight3", rateWeight);

					} else if (i == 3) {
						doc.append("stdRate4", trueRate);
						doc.append("rateWeight4", rateWeight);

					} else if (i == 4) {
						doc.append("stdRate5", trueRate);
						doc.append("rateWeight5", rateWeight);

					} else if (i == 5) {
						doc.append("stdRate6", trueRate);
						doc.append("rateWeight6", rateWeight);

					} else if (i == 6) {
						doc.append("stdRate7", trueRate);
						doc.append("rateWeight7", rateWeight);

					}

					sList.add(doc);

					doc = null;

				}

				// 분할 일괄저장하기
				super.insertMany(nColNm, sList, 10000);

				sList = null;

				log.info("#################################################################");
				log.info("# Save QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4\" Analysis Result!!");
				log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
				log.info("#################################################################");

			}

			/*
			 * #############################################################################
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 정답률 가중치 계산값이 저장되는 컬렉션 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep4 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 정답률 및 가중치 적용된 정답률 데이터 구조 생성 끝 !
			 * #############################################################################
			 */

			result = 1; // 데이터 처리가 성공하면 1로 값 변경

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doAnalysisStep4 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep4 End!");
		return result;
	}

	/**
	 * 정답률 분석 5단계
	 * 
	 * 퀴즈팩별 가중치가 적용된 정답률 최종 결과를 RDBMS에 저장
	 */
	private int doAnalysisStep5() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep5 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 퀴즈팩별 가중치가 적용된 정답률 최종 결과가 저장되는 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 퀴즈팩별 가중치가 적용된 정답률 최종 결과가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP5";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			// 퀴즈팩별 가중치가 적용된 정답률 최종 결과가 저장되는 컬렉션 생성
			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 퀴즈팩별 가중치가 적용된 정답률 최종 결과가 저장되는 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 생성 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_TRUE_RATE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4");

			// 데이터분석 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document().append("qp_id", "$qp_id"))
									.append("SUM(stdRate1)", new Document().append("$sum", "$stdRate1"))
									.append("SUM(stdRate2)", new Document().append("$sum", "$stdRate2"))
									.append("SUM(stdRate3)", new Document().append("$sum", "$stdRate3"))
									.append("SUM(stdRate4)", new Document().append("$sum", "$stdRate4"))
									.append("SUM(stdRate5)", new Document().append("$sum", "$stdRate5"))
									.append("SUM(stdRate6)", new Document().append("$sum", "$stdRate6"))
									.append("SUM(stdRate7)", new Document().append("$sum", "$stdRate7"))
									.append("SUM(rateWeight1)", new Document().append("$sum", "$rateWeight1"))
									.append("SUM(rateWeight2)", new Document().append("$sum", "$rateWeight2"))
									.append("SUM(rateWeight3)", new Document().append("$sum", "$rateWeight3"))
									.append("SUM(rateWeight4)", new Document().append("$sum", "$rateWeight4"))
									.append("SUM(rateWeight5)", new Document().append("$sum", "$rateWeight5"))
									.append("SUM(rateWeight6)", new Document().append("$sum", "$rateWeight6"))
									.append("SUM(rateWeight7)", new Document().append("$sum", "$rateWeight7"))),
					new Document().append("$project", new Document().append("qp_id", "$_id.qp_id")
							.append("stdRate1", "$SUM(stdRate1)").append("stdRate2", "$SUM(stdRate2)")
							.append("stdRate3", "$SUM(stdRate3)").append("stdRate4", "$SUM(stdRate4)")
							.append("stdRate5", "$SUM(stdRate5)").append("stdRate6", "$SUM(stdRate6)")
							.append("stdRate7", "$SUM(stdRate7)").append("rateWeight1", "$SUM(rateWeight1)")
							.append("rateWeight2", "$SUM(rateWeight2)").append("rateWeight3", "$SUM(rateWeight3)")
							.append("rateWeight4", "$SUM(rateWeight4)").append("rateWeight5", "$SUM(rateWeight5)")
							.append("rateWeight6", "$SUM(rateWeight6)").append("rateWeight7", "$SUM(rateWeight7)")
							.append("_id", 0)));

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			while (cursor.hasNext()) {

				final Document current = cursor.next();

				doc = new Document();

				// 퀴즈팩 아이디
				doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));

				// 퀴즈팩별 기준 정답률
				final double stdRate1 = NumberUtil.getDouble(current.get("stdRate1"));
				final double stdRate2 = NumberUtil.getDouble(current.get("stdRate2"));
				final double stdRate3 = NumberUtil.getDouble(current.get("stdRate3"));
				final double stdRate4 = NumberUtil.getDouble(current.get("stdRate4"));
				final double stdRate5 = NumberUtil.getDouble(current.get("stdRate5"));
				final double stdRate6 = NumberUtil.getDouble(current.get("stdRate6"));
				final double stdRate7 = NumberUtil.getDouble(current.get("stdRate7"));

				final double sumStdRate = stdRate1 + stdRate2 + stdRate3 + stdRate4 + stdRate5 + stdRate6 + stdRate7;

				// 최근 7일동안 평균 정답률
				final double stdRate = Math.round(sumStdRate / ContReadCntDay * ContDataSize) / ContDataSize;

				doc.append("stdRate", stdRate);

				// 퀴즈팩별 정답률
				final double rateWeight1 = NumberUtil.getDouble(current.get("rateWeight1"));
				final double rateWeight2 = NumberUtil.getDouble(current.get("rateWeight2"));
				final double rateWeight3 = NumberUtil.getDouble(current.get("rateWeight3"));
				final double rateWeight4 = NumberUtil.getDouble(current.get("rateWeight4"));
				final double rateWeight5 = NumberUtil.getDouble(current.get("rateWeight5"));
				final double rateWeight6 = NumberUtil.getDouble(current.get("rateWeight6"));
				final double rateWeight7 = NumberUtil.getDouble(current.get("rateWeight7"));

				final double sumRateWeight = rateWeight1 + rateWeight2 + rateWeight3 + rateWeight4 + rateWeight5
						+ rateWeight6 + rateWeight7;

				// 최근 7일동안의 가중치가 부여된 평균 정답률
				final double rateWeight = Math.round(sumRateWeight / ContReadCntDay * ContDataSize) / ContDataSize;

				// 퀴즈팩별 가중치가 부여된 정답률
				doc.append("rateWeight", rateWeight);

				sList.add(doc);

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 퀴즈 정답률 최종결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep5 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 퀴즈 정답률 구현을 위한 일자별 데이터가 저장되는 컬렉션 데이터 구조 생성 끝 !
			 * #############################################################################
			 */

			result = 1; // 데이터 처리가 성공하면 1로 값 변경

		} catch (Exception e) {
			log.info(this.getClass().getName() + ".doAnalysisStep5 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep5 End!");
		return result;
	}

	/**
	 * 이전 날짜의 사용하지 않는 데이터 삭제
	 * 
	 * 3단계 분석 결과는 14일 유지
	 */
	private int doCleanData() throws Exception {

		log.info(this.getClass().getName() + ".doCleanData Start!");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			String cleanDay = DateUtil.getDateTimeAdd(-1); // 삭제 날짜(하루 전)
			String dropColNm = "";

			// 1단계 분석 삭제
			dropColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + cleanDay + "_STEP1";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 2단계 분석 삭제
			dropColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + cleanDay + "_STEP2";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 4단계 분석 삭제
			dropColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + cleanDay + "_STEP4";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 5단계 분석 삭제
			dropColNm = "QUIZRANK_TRUE_RATE_ANALYSIS_" + cleanDay + "_STEP5";

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
	public int doAnalysis() throws Exception {
		int res = 0;

		/*
		 * #############################################################################
		 * 정답률 분석 1단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep1() != 1) {
			log.info("doAnalysisStep1 Fail !!");

			return 0;
		}
		/*
		 * #############################################################################
		 * 정답률 분석 1단계 수행 끝!!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 정답률 분석 2단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep2() != 1) {
			log.info("doAnalysisStep2 Fail !!");

			return 0;
		}
		/*
		 * #############################################################################
		 * 정답률 분석 2단계 수행 끝!!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 정답률 분석 3단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep3() != 1) {
			log.info("doAnalysisStep3 Fail !!");

			return 0;
		}
		/*
		 * #############################################################################
		 * 정답률 분석 3단계 수행 끝!!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 정답률 분석 4단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep4() != 1) {
			log.info("doAnalysisStep4 Fail !!");

			return 0;
		}
		/*
		 * #############################################################################
		 * 정답률 분석 4단계 수행 끝!!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 정답률 분석 5단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep5() != 1) {
			log.info("doAnalysisStep5 Fail !!");

			return 0;
		}
		/*
		 * #############################################################################
		 * 정답률 분석 5단계 수행 끝!!
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

		return res;
	}

}
