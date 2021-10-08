package poly.service.impl;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import poly.service.IBigDataService;
import poly.service.impl.analysis.rank.IQuizRankService;
import poly.service.impl.comm.ICommCont;
import poly.service.impl.comm.MongoDBComon;
import poly.util.DateUtil;

@Service("BigDataService")
public class BigDataService extends MongoDBComon implements IBigDataService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	// 퀴즈 정답률 분석 서비스 객체 생성
	@Resource(name = "QuizRankService")
	private IQuizRankService quizRankService;

	@Scheduled(cron = "0 10 5-8 * * *")
	@Override
	public void doDataAnalysis() throws Exception {

		log.info(this.getClass().getName() + ".doDataAnalysis Start!!");

		// 빅데이터 분석 시작 시간
		long startTime = System.currentTimeMillis();

		String toDay = DateUtil.getDateTime("yyyyMMdd");

		// 퀴즈팩, 풀이로그 기본 데이터셋 완성되면 실행
		if (super.doAnalysisStart(toDay, "qPackLogDataSet")) {

			// 내 수행 작업이 완료되지 않았으면 실행함
			if (!super.doAnalysisStart(toDay, "qBestRank")) {
				if (quizRankService.doDataAnalysis() != 1) {
					log.info(this.getClass().getName() + "doDataAnalysis Fail !!");
					return;

				}

				// 작업완료 상태 추가
				super.addAnalysisStatusEnd(toDay, "qBestRank");
			}
		}

		// 빅데이터 분석 종료 시간
		long endTime = System.currentTimeMillis();

		log.info("BigData Analysis Execute Time(s) : " + (endTime - startTime) / 1000.0 + " sec");

		log.info(this.getClass().getName() + ".doDataAnalysis End!");

		return;
	}

}
