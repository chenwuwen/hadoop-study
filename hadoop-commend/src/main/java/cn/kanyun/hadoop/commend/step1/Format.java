package cn.kanyun.hadoop.commend.step1;
/**
 * 格式化：如  小明+小丽  转换为 小丽+小明
 * @author root
 *
 */
public class Format {
	public String strFormat(String str1, String str2) {
		int c = str1.compareTo(str2);
		if (c < 0) {
			return str2 + str1;
		}
		return str1 + str2;
	}
}
