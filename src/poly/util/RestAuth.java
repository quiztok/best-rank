package poly.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RestAuth {

	private String ip = "";
	private String hostNm = "";

	public RestAuth() {

		InetAddress ipAddr;

		try {
			ipAddr = InetAddress.getLocalHost();
			this.ip = CmmUtil.nvl(ipAddr.getHostAddress()); // IP주소
			this.hostNm = CmmUtil.nvl(ipAddr.getHostName()); // 호스트명

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public RestAuth(String hostNm, String ip) {
		this.ip = ip;
		this.hostNm = hostNm;

	}

	public String doGenerateAuthKey() {

		String res = "";

		try {
			res = EncryptUtil.encHashSHA256(this.ip + this.hostNm + DateUtil.getDateTime());
			
		} catch (Exception e) {
			System.out.println(e.toString());
		}

		return res;

	}

}
