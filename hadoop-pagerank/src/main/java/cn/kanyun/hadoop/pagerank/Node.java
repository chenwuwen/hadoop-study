package cn.kanyun.hadoop.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {
	// 字符串第一个元素初始值为1
	private double pageRank = 1.0;
	// 字符串中，后面节点列表数组
	private String[] adjacentNodeNames;
	// PR值与数组分隔符
	private static final char fieldSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

	public static char getFieldseparator() {
		return fieldSeparator;
	}

	public boolean containsAdjacentNodes() {
		// TODO Auto-generated method stub
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	public static Node fromMR(String value) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value,fieldSeparator);
		if (parts.length<1) {
			throw new IOException("Expected 1 or more parts but received"+parts.length);
		}
//		1.0 A B C D
		Node node = new Node().setPageRank(Double.valueOf(parts[0]));
		if (parts.length>1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}

}
