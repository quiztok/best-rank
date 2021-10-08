package poly.persistance.redis.impl;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import poly.dto.redis.RankResultDTO;
import poly.persistance.redis.ICacheRedisMapper;
import poly.persistance.redis.comm.AbstractCommRedis;
import poly.util.DateUtil;

@Component("CacheRedisMapper")
public class CacheRedisMapper extends AbstractCommRedis implements ICacheRedisMapper {

	// 인기퀴즈, 사용자 연령대 분석 결과 저장
	@Resource(name = "MainRedisDB1")
	public RedisTemplate<String, Object> mainRedisDB1;

	// 개인별 추천 퀴즈 결과 저장
	@Resource(name = "MainRedisDB2")
	public RedisTemplate<String, Object> mainRedisDB2;

	private Logger log = Logger.getLogger(this.getClass());

	/**
	 * 분석 결과 등록하기
	 */
	@Override
	public int insertRankResult(RankResultDTO pDTO, int rank) throws Exception {

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
//		log.info(this.getClass().getName() + ".insertRankResult Start!");

		int res = 0;

		// 저장되는 Key 이름
		String redisKey = "QUIZRANK_" + DateUtil.getDateTime("yyyyMMdd");

		// String 타입
		mainRedisDB1.setKeySerializer(new StringRedisSerializer());

		// JSON 타입
		mainRedisDB1.setValueSerializer(new Jackson2JsonRedisSerializer<>(RankResultDTO.class));

		mainRedisDB1.opsForZSet().add(redisKey, pDTO, rank);

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
//		log.info(this.getClass().getName() + ".insertRankResult End!");

		res = 1;

		return res;
	}

	/**
	 * 등록된 분석 결과 가져오기
	 */
	@Override
	public int deleteRankResult() throws Exception {

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
		log.info(this.getClass().getName() + ".deleteRankResult Start!");

		int res = 0;

		// 저장되는 Key 이름
		String redisKey = "QUIZRANK_" + DateUtil.getDateTime("yyyyMMdd");

		mainRedisDB1.delete(redisKey);

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
		log.info(this.getClass().getName() + ".deleteRankResult End!");

		res = 1;

		return res;
	}

	/**
	 * 인기퀴즈 TTL 기간 설정하기
	 */
	@Override
	public int setQuizRankExpire() throws Exception {
		log.info(this.getClass().getName() + ".setDayExpire Start!");

		int res = 0;

		try {
			String redisKey = "QUIZRANK_" + DateUtil.getDateTime("yyyyMMdd");

			mainRedisDB1.expire(redisKey, 2, TimeUnit.DAYS);

			res = 1;

		} catch (Exception e) {
			res = 0;
		}

		log.info(this.getClass().getName() + ".setDayExpire End!");

		return res;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Set<RankResultDTO> getRankResult(int rank) throws Exception {

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
		log.info(this.getClass().getName() + ".getRankResult Start!");

		// 저장된 데이터
		String redisKey = "QUIZRANK_" + DateUtil.getDateTime("yyyyMMdd");

		Set<RankResultDTO> rSet = null;

		// String 타입
		mainRedisDB1.setKeySerializer(new StringRedisSerializer());

		// JSON 타입
		mainRedisDB1.setValueSerializer(new Jackson2JsonRedisSerializer<>(RankResultDTO.class));

		// 분석 결과 조회하기
		if (mainRedisDB1.hasKey(redisKey)) { // 데이터가 존재한다면

			// 결과 가져오기
			rSet = (Set) mainRedisDB1.opsForZSet().range(redisKey, 0, rank);

		} else { //// 데이터가 존재하지 않는다면

			redisKey = "QUIZRANK_" + DateUtil.getDateTimeAdd(-1);

			// 어제 날짜 분석 결과 조회하기
			if (mainRedisDB1.hasKey(redisKey)) { // 데이터가 존재한다면

				// 결과 가져오기
				rSet = (Set) mainRedisDB1.opsForZSet().range(redisKey, 0, rank);

			}

		}

		// 로그 찍기(추후 찍은 로그를 통해 이 함수에 접근했는지 파악하기 용이하다.)
		log.info(this.getClass().getName() + ".getRankResult End!");

		return rSet;
	}

}
