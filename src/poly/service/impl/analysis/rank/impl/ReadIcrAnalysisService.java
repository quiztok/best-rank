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

import poly.service.impl.analysis.rank.IReadIcrAnalysisService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.util.CmmUtil;
import poly.util.DateUtil;
import poly.util.NumberUtil;

@Service("ReadIcrAnalysisService")
public class ReadIcrAnalysisService extends MongoDBComon implements IReadIcrAnalysisService, ICommCont {

	/*
	 * #############################################################################
	 * 정의된 상수
	 * #############################################################################
	 */

	// 오늘의 최대 조회수수
	private double maxValue = 0.0;

	// 오늘의 평균 조회수수
	private double avgValue = 0.0;

	// 가중치 기준 조회수수
	private double stdValue = 0.0;

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	/**
	 * 분석 1단계
	 * 
	 * 오늘과 어제 조회율 변화 계산
	 */
	private int doAnalysisStep1() throws Exception {

		log.info(this.getClass().getName() + ".doAnalysisStep1 Start!");

		String stdDay = DateUtil.getDateTime("yyyyMMdd"); // 분석 날짜
		String preDay = DateUtil.getDateTimeAdd(-1); // 어제 날짜

		int result = 0; // 데이터 처리가 성공하면 1로 값 변경

		try {

			/*
			 * #############################################################################
			 * 컬렉션 생성 시작!
			 * #############################################################################
			 */

			// 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + stdDay + "_STEP1";

			// 기존에 등록된 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 삭제
			if (mongodb.collectionExists(nColNm)) {
				mongodb.dropCollection(nColNm);

			}

			mongodb.createCollection(nColNm).createIndex(Indexes.ascending("qp_id"));
			;

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

			String stdColNm = "QUIZRANK_DATA_AN_" + stdDay; // 기준 컬렌션
			String joinColNm = "QUIZRANK_DATA_AN_" + preDay + "_ALL"; // 조인 대상 컬렉션

			MongoCollection<Document> rCol = mongodb.getCollection(stdColNm);

			// 데이터 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$project", new Document().append("_id", 0).append(stdColNm, "$$ROOT")),
					new Document().append("$lookup",
							new Document().append("localField", stdColNm + ".dt_id").append("from", joinColNm)
									.append("foreignField", "dt_id").append("as", joinColNm)),
					new Document().append("$unwind",
							new Document().append("path", "$" + joinColNm).append("preserveNullAndEmptyArrays", true)),
					new Document().append("$project",
							new Document().append("dt_id", "$" + stdColNm + ".dt_id")
									.append("cur_click_cnt", "$" + stdColNm + ".click_cnt")
									.append("pre_click_cnt", "$" + joinColNm + ".click_cnt").append("_id", 0)));

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
				String qp_id = CmmUtil.nvl((String) current.get("dt_id"));

				// 오늘 조회수(계산을 위해 값이 Null이면 0이 아닌 1로 값 설정)
				long cur_click_cnt = CmmUtil.nvl(current.get("cur_click_cnt"), 1);

				// 어제 조회수(계산을 위해 값이 Null이면 0이 아닌 1로 값 설정)
				long pre_click_cnt = CmmUtil.nvl(current.get("pre_click_cnt"), 1);

				double rate = Math.round(((double) cur_click_cnt / (double) pre_click_cnt) * ContDataSize)
						/ ContDataSize;

//				log.info("cur_click_cnt : " + cur_click_cnt);
//				log.info("pre_click_cnt : " + pre_click_cnt);
//				log.info("rate : " + rate);

				doc.append("qp_id", qp_id);
				doc.append("cur_click_cnt", cur_click_cnt);
				doc.append("pre_click_cnt", pre_click_cnt);
				doc.append("rate", rate);

				sList.add(doc); // 저장할 데이터 저장

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 조회율이 저장되는 컬렉션 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep1 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 데이터 생성 끝 !
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
	 * 조회율 가중치 부여를 위한 기준값 구현
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

			// 오늘의 퀴즈팩별 최대 진행율 데이터가 저장되는 컬렉션 이름
			String nColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + ContAnaysisStdDay + "_STEP2";

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

			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_READ_ICR_ANALYSIS_" + ContAnaysisStdDay + "_STEP1");

			// 데이터 쿼리 생성하기
			List<? extends Bson> pipeline = Arrays.asList(
					new Document().append("$group",
							new Document().append("_id", new Document())
									.append("MAX(rate)", new Document().append("$max", "$rate"))
									.append("AVG(rate)", new Document().append("$avg", "$rate"))),
					new Document().append("$project", new Document().append("max_rate", "$MAX(rate)")
							.append("avg_rate", "$AVG(rate)").append("_id", 0)));

			AggregateIterable<Document> rs = rCol.aggregate(pipeline).allowDiskUse(true);
			Iterator<Document> cursor = rs.iterator();

			Document doc = null;

			// 퀴즈팩별 좋아요율 일자별 저장하기
			if (cursor.hasNext()) {

				final Document current = cursor.next();

				doc = new Document();

				// 최대 조회 증가율
				this.maxValue = NumberUtil.getDouble(current.get("max_rate"));

				// 평균 조회 증가율
				this.avgValue = NumberUtil.getDouble(current.get("avg_rate"));

				// 기준 조회 증가율
				this.stdValue = Math.round(((this.avgValue + this.maxValue) / 2) * ContDataSize) / ContDataSize;

				// 조회율 저장하기

				doc.append("maxValue", this.maxValue);
				doc.append("avgValue", this.avgValue);
				doc.append("stdValue", this.stdValue);

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
			log.info(this.getClass().getName() + ".doAnalysisStep2 Error : " + e.toString());
			result = 0;
		}

		log.info(this.getClass().getName() + ".doAnalysisStep2 End!");
		return result;
	}

	/**
	 * 분석 3단계
	 * 
	 * 최근 조회 증가율에 대한 가중치 계산한
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

			// 컬렉션 이름
			String nColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + ContAnaysisStdDay + "_STEP3";

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

			// 퀴즈팩별 정답률 데이터 가져오기
			MongoCollection<Document> rCol = mongodb
					.getCollection("QUIZRANK_READ_ICR_ANALYSIS_" + ContAnaysisStdDay + "_STEP1");

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

				// 퀴즈팩 조회 증가율
				double rate = NumberUtil.getDouble(current.get("rate"));
				doc.append("stdRate", rate);

				// 퀴즈팩 조회 증가율 가중치 계산
				double rateWeight = Math.round((rate / this.stdValue) * ContDataSize) / ContDataSize;

//				log.info("rate : " + rate);
//				log.info("stdValue : " + this.stdValue);
//				log.info("rateWeight : " + rateWeight);

				// 가중치 계산된 값이 1보다 크면, 가중치 최대 값인 1로 변경함
				if (rateWeight > 1) {
					rateWeight = 1;

				}

				doc.append("rateWeight", rateWeight);

				sList.add(doc); // 저장할 데이터 저장

				doc = null;

			}

			// 분할 일괄저장하기
			super.insertMany(nColNm, sList, 10000);

			sList = null;

			/*
			 * #############################################################################
			 * 퀴즈팩 전체 정답률을 추가하기 위한 퀴즈팩별 좋아요수 가중치 계산값이 저장되는 컬렉션 데이터 생성 결과 통계
			 * #############################################################################
			 */
			log.info("#################################################################");
			log.info("# doAnalysisStep3 Result!!");
			log.info("# " + nColNm + " insert Doc Count : " + mongodb.getCollection(nColNm).countDocuments());
			log.info("#################################################################");

			/*
			 * #############################################################################
			 * 데이터 생성 끝 !
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
			dropColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + cleanDay + "_STEP1";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 2단계 분석 삭제
			dropColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + cleanDay + "_STEP2";

			log.info("Drop Collection : " + dropColNm + " Start! ");
			if (mongodb.collectionExists(dropColNm)) {
				mongodb.dropCollection(dropColNm);
				log.info("Drop Collection : " + dropColNm);

			} else {
				log.info("Not Exists Collection : " + dropColNm);

			}
			log.info("Drop Collection : " + dropColNm + " End! ");

			// 3단계 분석 삭제
			dropColNm = "QUIZRANK_READ_ICR_ANALYSIS_" + cleanDay + "_STEP3";

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
