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

import poly.service.impl.analysis.rank.IShareAnalysisService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.util.CmmUtil;
import poly.util.DateUtil;
import poly.util.NumberUtil;

@Service("ShareAnalysisService")
public class ShareAnalysisService extends MongoDBComon implements IShareAnalysisService, ICommCont {

	/*
	 * #############################################################################
	 * 정의된 상수
	 * #############################################################################
	 */

	// 오늘의 최대 공유하기수
	private long maxValue = 0;

	// 오늘의 평균 공유하기수
	private double avgValue = 0;

	// 가중치 기준 공유하기수
	private double stdValue = 0;

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 분석 1단계
	 * 
	 * 퀴즈팩별 오늘 최대, 평군, 기준 공유하기 분석
	 */
	private int doAnalysisStep1() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep1 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP1";

			// 기존에 등록된 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			mongodb.createCollection(nColNm);

			/*
			 * #############################################################################
			 * 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 데이터 생성하기 시작!
			 * #############################################################################
			 */

			MongoCollection<Document> rCol = mongodb.getCollection("QUIZRANK_DATA_AN_" + ContAnaysisStdDay);

			// 데이터 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document())
									.append("MAX(share_cnt)", new Document().append("$max", "$share_cnt"))
									.append("AVG(share_cnt)", new Document().append("$avg", "$share_cnt"))),
					new Document().append("$project", new Document().append("max_cnt", "$MAX(share_cnt)")
							.append("avg_cnt", "$AVG(share_cnt)").append("_id", 0)));

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			if (cursor.hasNext()) {

				final Document current = cursor.next();

				// 오늘의 퀴즈팩별 최대 공유하기율
				this.maxValue = NumberUtil.getLong(current.get("max_cnt"));

				// 오늘의 퀴즈팩별 평균 공유하기율
				this.avgValue = NumberUtil.getDouble(current.get("avg_cnt"));

				// 오늘의 퀴즈팩별 기준 공유하기율
				this.stdValue = Math.round(((this.avgValue + this.maxValue) / 2) * ContDataSize) / ContDataSize;

				Document doc = new Document();

				doc.append("std_date", ContAnaysisStdDay);
				doc.append("maxValue", maxValue);
				doc.append("avgValue", avgValue);
				doc.append("stdValue", stdValue);

				MongoCollection<Document> col = mongodb.getCollection(nColNm);
				col.insertOne(new Document(doc));

				doc = null;
				col = null;

			}

			/*
			 * #############################################################################
			 * 데이터 생성하기 끝!
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
	 * 분석 2단계
	 * 
	 * 퀴즈팩별 가중치 부여 - 1단계에서 생성된 QUIZ_PACK_INFO_yyyyMMdd 데이터로부터 분석 - 3차 데이터분석 진행
	 */
	private int doAnalysisStep2() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep2 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 컬렉션 이름
			String nColNm = "QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP2";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 데이터 생성 시작 !
			 * #############################################################################
			 */

			MongoCollection<Document> rCol = mongodb.getCollection("QUIZRANK_DATA_AN_" + ContAnaysisStdDay);

			FindIterable<Document> rs = rCol.find();
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			while (cursor.hasNext()) {

				doc = new Document();

				final Document current = cursor.next();

				// 퀴즈팩 아이디(배포아이디를 퀴즈팩 아이디로 사용함)
				String qp_id = CmmUtil.nvl((String) current.get("dt_id"));
				doc.append("qp_id", qp_id);

				// 퀴즈팩별 중복되지 않는 좋아요수
				long cnt = (long) current.get("share_cnt");
				doc.append("cnt", cnt);

				// 전체 퀴즈팩 기준 좋아요율과 퀴즈팩별 좋아요수에 대한 가중치 계산
				double rateWeight = Math.round((cnt / this.stdValue) * ContDataSize) / ContDataSize;

				// 가중치 계산된 값이 1보다 크면, 가중치 최대 값인 1로 변경함
				if (rateWeight > 1) {
					rateWeight = 1;

				}

				doc.append("rate", rateWeight);

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
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 좋아요수 가중치 계산값이 저장되는 컬렉션 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep2 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 데이터 생성 끝 !
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
	 * 분석 3단계
	 * 
	 * 최근 7일동안의 일자별 퀴즈팩들의 공유하기 및 가중치 적용된 공유하기률 데이터 구조 생성
	 */
	private int doAnalysisStep3() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep3 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 컬렉션 생성 시작!
			 * #############################################################################
			 */

			String nColNm = "QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP3";

			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 공유하기 및 가중치 적용된 공유하기 데이터 구조 생성 시작!!
			 * #############################################################################
			 */

			for (int i = 0; i < ContReadCntDay; i++) {

				String stdDay = DateUtil.getDateTimeAdd(-i); // 분석 일자

				// 이전 좋아요수 저장 컬렉션 로드하기
				MongoCollection<Document> rCol = mongodb.getCollection("QUIZRANK_SHARE_ANALYSIS_" + stdDay + "_STEP2");

				log.info("#################################################################");
				log.info("# Read QUIZRANK_SHARE_ANALYSIS_" + stdDay + "_STEP2 Analysis Start!!");
				log.info("#################################################################");

				FindIterable<Document> rs = rCol.find();
				Iterator<Document> cursor = rs.iterator();

				Document doc = null;

				// 일괄저장을 위한 객체 생성
				List<Document> sList = new ArrayList<Document>();

				while (cursor.hasNext()) {

					doc = new Document();

					final Document current = cursor.next();

					String qp_id = CmmUtil.nvl((String) current.get("qp_id")); // 퀴즈팩 아이디
					doc.append("qp_id", qp_id);

					long cnt = NumberUtil.getLong(current.get("cnt"));
					double rate = NumberUtil.getDouble(current.get("rate"));

					if (i == 0) {
						doc.append("stdCnt1", cnt);
						doc.append("cntWeight1", rate);

					} else if (i == 1) {
						doc.append("stdCnt2", cnt);
						doc.append("cntWeight2", rate);

					} else if (i == 2) {
						doc.append("stdCnt3", cnt);
						doc.append("cntWeight3", rate);

					} else if (i == 3) {
						doc.append("stdCnt4", cnt);
						doc.append("cntWeight4", rate);

					} else if (i == 4) {
						doc.append("stdCnt5", cnt);
						doc.append("cntWeight5", rate);

					} else if (i == 5) {
						doc.append("stdCnt6", cnt);
						doc.append("cntWeight6", rate);

					} else if (i == 6) {
						doc.append("stdCnt7", cnt);
						doc.append("cntWeight7", rate);

					}

					sList.add(doc); // 저장할 데이터 저장

					doc = null;

				}

				// 분할 일괄저장하기
				super.insertMany(nColNm, sList, 10000);

				sList = null;

				cursor = null;
				rs = null;

				log.info("#################################################################");
				log.info("# Save QUIZRANK_SHARE_ANALYSIS_" + stdDay + "_STEP3 Analysis Result!!");
				log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
				log.info("#################################################################");

			}

			/*
			 * #############################################################################
			 * 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep3 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 최근 7일동안의 일자별 퀴즈팩들의 공유하기 및 가중치 적용된 공유하기 데이터 구조 생성 끝!!
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
	 * 분석 4단계
	 * 
	 * 퀴즈팩별 가중치가 적용된 공유하기 최종 결과 생성
	 */
	private int doAnalysisStep4() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep4 Start!");

		String ContAnaysisStdDay = DateUtil.getDateTime("yyyyMMdd");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 컬렉션 생성 시작!
			 * #############################################################################
			 */

			String nColNm = "QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP4";

			// 기존에 잘못 등록된 컬렉션이 존재할 수 있기 때문에 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));

			/*
			 * #############################################################################
			 * 컬렉션 생성 끝!
			 * #############################################################################
			 */

			/*
			 * #############################################################################
			 * 데이터 구조 생성 시작 !
			 * #############################################################################
			 */

			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_SHARE_ANALYSIS_" + ContAnaysisStdDay + "_STEP3");

			// 데이터분석 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document().append("qp_id", "$qp_id"))
									.append("SUM(stdCnt1)", new Document().append("$sum", "$stdCnt1"))
									.append("SUM(stdCnt2)", new Document().append("$sum", "$stdCnt2"))
									.append("SUM(stdCnt3)", new Document().append("$sum", "$stdCnt3"))
									.append("SUM(stdCnt4)", new Document().append("$sum", "$stdCnt4"))
									.append("SUM(stdCnt5)", new Document().append("$sum", "$stdCnt5"))
									.append("SUM(stdCnt6)", new Document().append("$sum", "$stdCnt6"))
									.append("SUM(stdCnt7)", new Document().append("$sum", "$stdCnt7"))
									.append("SUM(cntWeight1)", new Document().append("$sum", "$cntWeight1"))
									.append("SUM(cntWeight2)", new Document().append("$sum", "$cntWeight2"))
									.append("SUM(cntWeight3)", new Document().append("$sum", "$cntWeight3"))
									.append("SUM(cntWeight4)", new Document().append("$sum", "$cntWeight4"))
									.append("SUM(cntWeight5)", new Document().append("$sum", "$cntWeight5"))
									.append("SUM(cntWeight6)", new Document().append("$sum", "$cntWeight6"))
									.append("SUM(cntWeight7)", new Document().append("$sum", "$cntWeight7"))),
					new Document().append("$project",
							new Document().append("qp_id", "$_id.qp_id").append("stdCnt1", "$SUM(stdCnt1)")
									.append("stdCnt2", "$SUM(stdCnt2)").append("stdCnt3", "$SUM(stdCnt3)")
									.append("stdCnt4", "$SUM(stdCnt4)").append("stdCnt5", "$SUM(stdCnt5)")
									.append("stdCnt6", "$SUM(stdCnt6)").append("stdCnt7", "$SUM(stdCnt7)")
									.append("cntWeight1", "$SUM(cntWeight1)").append("cntWeight2", "$SUM(cntWeight2)")
									.append("cntWeight3", "$SUM(cntWeight3)").append("cntWeight4", "$SUM(cntWeight4)")
									.append("cntWeight5", "$SUM(cntWeight5)").append("cntWeight6", "$SUM(cntWeight6)")
									.append("cntWeight7", "$SUM(cntWeight7)").append("_id", 0)));

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 일괄저장을 위한 객체 생성
			List<Document> sList = new ArrayList<Document>();

			// 퀴즈팩별 좋아요율 일자별 저장하기
			while (cursor.hasNext()) {

				doc = new Document();

				final Document current = cursor.next();

				// 퀴즈팩 아이디
				doc.append("qp_id", CmmUtil.nvl((String) current.get("qp_id")));

				// 퀴즈팩별 좋아요수
				final double stdCnt1 = NumberUtil.getDouble(current.get("stdCnt1"));
				final double stdCnt2 = NumberUtil.getDouble(current.get("stdCnt2"));
				final double stdCnt3 = NumberUtil.getDouble(current.get("stdCnt3"));
				final double stdCnt4 = NumberUtil.getDouble(current.get("stdCnt4"));
				final double stdCnt5 = NumberUtil.getDouble(current.get("stdCnt5"));
				final double stdCnt6 = NumberUtil.getDouble(current.get("stdCnt6"));
				final double stdCnt7 = NumberUtil.getDouble(current.get("stdCnt7"));

				final double sumStdCnt = stdCnt1 + stdCnt2 + stdCnt3 + stdCnt4 + stdCnt5 + stdCnt6 + stdCnt7;

				// 최근 7일동안 퀴즈팩 좋아요수 합계
				final double stdCnt = Math.round(sumStdCnt / ContReadCntDay * ContDataSize) / ContDataSize;

				// 최근 7일동안의 평균 좋아요수
				doc.append("stdRate", stdCnt);

				// 퀴즈팩별 조회율
				final double cntWeight1 = NumberUtil.getDouble(current.get("cntWeight1"));
				final double cntWeight2 = NumberUtil.getDouble(current.get("cntWeight2"));
				final double cntWeight3 = NumberUtil.getDouble(current.get("cntWeight3"));
				final double cntWeight4 = NumberUtil.getDouble(current.get("cntWeight4"));
				final double cntWeight5 = NumberUtil.getDouble(current.get("cntWeight5"));
				final double cntWeight6 = NumberUtil.getDouble(current.get("cntWeight6"));
				final double cntWeight7 = NumberUtil.getDouble(current.get("cntWeight7"));

				// 최근 7일동안 가중치가 부여된 평균 좋아요율 합계
				final double sumCntWeight = cntWeight1 + cntWeight2 + cntWeight3 + cntWeight4 + cntWeight5 + cntWeight6
						+ cntWeight7;

				// 최근 7일동안의 가중치가 부여된 평균 좋아요율
				final double rate = Math.round(sumCntWeight / ContReadCntDay * ContDataSize) / ContDataSize;

				doc.append("rateWeight", rate);

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
			 * 퀴즈 좋아요율 최종결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep4 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 퀴즈 좋아요율 구현을 위한 일자별 데이터가 저장되는 컬렉션 데이터 구조 생성 끝 !
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
	 * 이전 날짜의 사용하지 않는 데이터 삭제
	 * 
	 * 2단계 분석 결과는 14일 유지
	 */
	private int doCleanData() throws Exception {

		log.info(this.getClass().getName() + ".doCleanData Start!");

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			String cleanDay = DateUtil.getDateTimeAdd(-1); // 삭제 날짜(하루 전)
			String dropColNm = "";

			// 1단계 분석 삭제
			dropColNm = "QUIZRANK_SHARE_ANALYSIS_" + cleanDay + "_STEP1";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 3단계 분석 삭제
			dropColNm = "QUIZRANK_SHARE_ANALYSIS_" + cleanDay + "_STEP3";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 4단계 분석 삭제
			dropColNm = "QUIZRANK_SHARE_ANALYSIS_" + cleanDay + "_STEP4";

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
		 * 분석 1단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep1() != 1) {
			log.info("doAnalysisStep1 Fail !!");

			return 0;
		}

		/*
		 * #############################################################################
		 * 분석 2단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep2() != 1) {
			log.info("doAnalysisStep2 Fail !!");

			return 0;
		}

		/*
		 * #############################################################################
		 * 분석 3단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep3() != 1) {
			log.info("doAnalysisStep3 Fail !!");

			return 0;
		}

		/*
		 * #############################################################################
		 * 분석 4단계 수행 시작!!
		 * #############################################################################
		 */
		if (doAnalysisStep4() != 1) {
			log.info("doAnalysisStep4 Fail !!");

			return 0;
		}

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
