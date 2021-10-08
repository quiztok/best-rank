package poly.persistance.redis;

import java.util.Set;

import poly.dto.redis.RankResultDTO;

public interface ICacheRedisMapper {

	/*
	 * #############################################################################
	 * 인기 퀴즈
	 * #############################################################################
	 */
	public int insertRankResult(RankResultDTO pDTO, int rank) throws Exception;

	public Set<RankResultDTO> getRankResult(int rank) throws Exception;

	public int deleteRankResult() throws Exception;

	public int setQuizRankExpire() throws Exception;

}
