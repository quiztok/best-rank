package poly.util;

public class NumberUtil {

	/* Wrapper 객체로 선언된 숫자형태 데이터를 강제로 형변환하기 */
	/**
	 * long 타입 변환하기
	 */
	public static long getLong(Object num) {
		Number res = (Number) num;
		return res.longValue();
	}

	/**
	 * int 타입 변환하기
	 */
	public static int getInt(Object num) {
		if (num==null) {
			num = 0;
		}
		
		Number res = (Number) num;
		return res.intValue();
	}

	/**
	 * double 타입 변환하기
	 */
	public static double getDouble(Object num) {
		Number res = (Number) num;
		return res.doubleValue();
	}

	/**
	 * float 타입 변환하기
	 */
	public static float getFloat(Object num) {
		Number res = (Number) num;
		return res.floatValue();
	}

}
