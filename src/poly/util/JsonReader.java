package poly.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonReader {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	/**
	 * API Body 내용 문자열로 변환하기
	 * 
	 * @param Reader
	 */
	private String readAll(Reader rd) {

		log.info("readAll Start!");

		StringBuilder sb = null;

		try {
			sb = new StringBuilder();
			int cp = 0;

			while ((cp = rd.read()) != -1) {
				sb.append((char) cp);
			}

		} catch (Exception e) {
			log.info("readAll Exception : " + e.toString());
		}

		log.info("readAll End!");

		return sb.toString();
	}

	/**
	 * URL로부터 JSON받아 객체 생성하기
	 * 
	 * @param URL
	 * @throws IOException 
	 */
	public JSONObject readJsonFromUrl(String url) throws IOException {

		log.info("readJsonFromUrl Start!");

		log.info("readJsonFromUrl url : " + url);

		BufferedReader rd = null;
		JSONParser parser = null;
		InputStream is = null;
		JSONObject json = null;

		try {
			is = new URL(url).openStream();
			rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

			String jsonText = readAll(rd);
			parser = new JSONParser();

			// JSON 파싱
			json = (JSONObject) parser.parse(jsonText);

		} catch (Exception e) {
			log.info("readJsonFromUrl Exception : " + e.toString());

		} finally {
			is.close();
			is = null;
			rd = null;
			parser = null;

		}

		log.info("readJsonFromUrl End!");

		return json;
	}
}
