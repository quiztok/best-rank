package poly.service.impl.analysis.rank;

/**
 * 인기 퀴즈 구현을 위한 서비스
 */
public interface IQuizRankService {

	// 인기 퀴즈 구현을 위한 데이터 분석
	public int doDataAnalysis() throws Exception;

	// 인기퀴즈 제외 키워드
	public String[] stopWord = { "요양보호사", "포인트", "3500" };
}
