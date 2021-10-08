package poly.service.impl;

import java.util.Set;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import poly.dto.redis.RankResultDTO;
import poly.persistance.redis.ICacheRedisMapper;
import poly.service.IResService;
import poly.service.impl.comm.ICommCont;

@Service("ResService")
public class ResService implements IResService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	// 인기 퀴즈 분석 최종 결과를 Redis에 저장
	@Resource(name = "CacheRedisMapper")
	private ICacheRedisMapper cacheRedisMapper;

	@Override
	public Set<RankResultDTO> getQuizRank(int rank) throws Exception {
		log.info(this.getClass().getName() + ".CacheRedisMapper Start!");

		return cacheRedisMapper.getRankResult(rank);
	}

}
