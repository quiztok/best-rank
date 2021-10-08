package poly.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	/**
	 * 날짜, 시간 출력하기
	 * 
	 * @param fm 날짜 출력 형식
	 * @return
	 */
	public static String getDateTimeAdd(int day) {

		return getDateTimeAdd("yyyyMMdd", day);

	}

	/**
	 * 날짜, 시간 출력하기
	 * 
	 * @param fm 날짜 출력 형식
	 * @return
	 */
	public static String getDateTimeAdd(String fm, int day) {

		SimpleDateFormat date = new SimpleDateFormat(fm);

		Calendar cal = Calendar.getInstance();
//        cal.add(Calendar.DATE, day-1);		//장애 대응용
		cal.add(Calendar.DATE, day);

		return date.format(cal.getTime());

	}

	/**
	 * 날짜, 시간 출력하기
	 * 
	 * @param fm 날짜 출력 형식
	 * @return
	 */
	public static String getDateTime(String fm) {

		Date today = new Date();
//		today.setDate(22); // 장애 대응용
		System.out.println(today);

		SimpleDateFormat date = new SimpleDateFormat(fm);

		return date.format(today);
	}

	/**
	 * 날짜, 시간 출력하기
	 * 
	 * @return 기본값은 년.월.일
	 */
	public static String getDateTime() {

		return getDateTime("yyyy.MM.dd");

	}

	/**
	 * 오늘 날짜의 요일 가져오기
	 * 
	 * @return 요일 값
	 */
	public static int getDayOfWeek() {

		Calendar cal = Calendar.getInstance();

		return cal.get(Calendar.DAY_OF_WEEK);

	}
}
