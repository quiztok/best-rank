package poly.persistance.redis.comm;

import org.apache.log4j.Logger;

public abstract class AbstractCommRedis {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	/**
	 * 회원 아이디별 저장될 그룹정보 가져오기
	 */
	protected int getPersonalAnalysisUserGroup(String user_id) {

		log.info(this.getClass().getName() + ".getPersonalAnalysisUserGroup Start!");

		// user_id의 첫글자만 가져오기
		char id = user_id.charAt(0);

		// 회원아이디 첫글자를 기반으로 그룹 나누기
		int grp = id % 16;

		log.info("id : " + id);
		log.info("grp : " + grp);

		log.info(this.getClass().getName() + ".getPersonalAnalysisUserGroup End!");

		return grp;
	}
}
