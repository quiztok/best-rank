package poly.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

public class CollectionUtil {

	public static List<Document> sortAsc(Map<String, Double> pMap, int rang) {

		// Map.Entry 리스트 작성
		List<Entry<String, Double>> tempList = new LinkedList<Entry<String, Double>>(pMap.entrySet());

		// 비교함수 Comparator를 사용하여 오름차순으로 정렬
		Collections.sort(tempList, new Comparator<Entry<String, Double>>() {
			// compare로 값을 비교
			public int compare(Entry<String, Double> obj1, Entry<String, Double> obj2) {
				return obj1.getValue().compareTo(obj2.getValue());
			}
		});

		Iterator<Entry<String, Double>> rIt = tempList.iterator();

		int idx = 0;

		List<Document> rList = new ArrayList<Document>();

		while (rIt.hasNext()) {
			Map.Entry<String, Double> entry = (Map.Entry<String, Double>) rIt.next();
			idx++;

			if (idx >= rang) {
				break;

			}

			Document doc = new Document();
			doc.append(entry.getKey(), entry.getValue());

			rList.add(doc);

			doc = null;
		}

		rIt = null;
		tempList = null;

		return rList;
	}

	public static Document sortDesc(Map<String, Double> pMap, int rang) {

		// Map.Entry 리스트 작성
		List<Entry<String, Double>> tempList = new LinkedList<Entry<String, Double>>(pMap.entrySet());

		// 비교함수 Comparator를 사용하여 오름차순으로 정렬
		Collections.sort(tempList, new Comparator<Entry<String, Double>>() {
			// compare로 값을 비교
			public int compare(Entry<String, Double> obj1, Entry<String, Double> obj2) {
				return obj2.getValue().compareTo(obj1.getValue());
			}
		});

		Iterator<Entry<String, Double>> rIt = tempList.iterator();

		int idx = 0;

		Document rDoc = new Document();
		while (rIt.hasNext()) {
			Map.Entry<String, Double> entry = (Map.Entry<String, Double>) rIt.next();
			idx++;

			if (idx >= rang) {
				break;

			}

			rDoc.append(entry.getKey(), entry.getValue());
//			rDoc.append("\"" + entry.getKey() + "\"", entry.getValue());

		}

		rIt = null;
		tempList = null;

		return rDoc;
	}

}
