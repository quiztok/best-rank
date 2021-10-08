package poly.service;

import java.util.Set;

import poly.dto.redis.RankResultDTO;

/**
 * 분석 결과를 제공하는 서비스
 */
public interface IResService {

	// 인기퀴즈 결과
	public Set<RankResultDTO> getQuizRank(int rank) throws Exception;

}
