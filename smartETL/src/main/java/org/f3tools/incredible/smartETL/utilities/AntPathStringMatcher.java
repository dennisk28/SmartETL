package org.f3tools.incredible.smartETL.utilities;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class exactly serves my need so thanks to the original author. Dennis Kang 2015/3/13
 * 
 * Package-protected helper class for {@link AntPathMatcher}. Tests whether or not a string matches against a pattern
 * using a regular expression.
 *
 * <p>The pattern may contain special characters: '*' means zero or more characters; '?' means one and only one
 * character; '{' and '}' indicate a URI template pattern.
 *
 * @author Arjen Poutsma
 */
public class AntPathStringMatcher {

	private static final Pattern GLOB_PATTERN = Pattern.compile("\\?|\\*|\\{([^/]+?)\\}");

	private static final String DEFAULT_VARIABLE_PATTERN = "(.*)";

	private final Pattern pattern;

	private String str;

	private final List<String> variableNames = new LinkedList<String>();

	private final Map<String, String> uriTemplateVariables;

	/** Construct a new instance of the <code>AntPatchStringMatcher</code>. */
	public AntPathStringMatcher(String pattern, String str, Map<String, String> uriTemplateVariables) {
		this.str = str;
		this.uriTemplateVariables = uriTemplateVariables;
		this.pattern = createPattern(pattern);
	}

	private Pattern createPattern(String pattern) {
		StringBuilder patternBuilder = new StringBuilder();
		Matcher m = GLOB_PATTERN.matcher(pattern);
		int end = 0;
		while (m.find()) {
			patternBuilder.append(quote(pattern, end, m.start()));
			String match = m.group();
			if ("?".equals(match)) {
				patternBuilder.append('.');
			}
			else if ("*".equals(match)) {
				patternBuilder.append(".*");
			}
			else if (match.startsWith("{") && match.endsWith("}")) {
				int colonIdx = match.indexOf(':');
				if (colonIdx == -1) {
					patternBuilder.append(DEFAULT_VARIABLE_PATTERN);
					variableNames.add(m.group(1));
				}
				else {
					String variablePattern = match.substring(colonIdx + 1, match.length() - 1);
					patternBuilder.append('(');
					patternBuilder.append(variablePattern);
					patternBuilder.append(')');
					String variableName = match.substring(1, colonIdx);
					variableNames.add(variableName);
				}
			}
			end = m.end();
		}
		patternBuilder.append(quote(pattern, end, pattern.length()));
		return Pattern.compile(patternBuilder.toString());
	}

	private String quote(String s, int start, int end) 
	{
		if (start == end) {
			return "";
		}
		return Pattern.quote(s.substring(start, end));
	}

	/**
	 * Main entry point.
	 *
	 * @return <code>true</code> if the string matches against the pattern, or <code>false</code> otherwise.
	 */
	public boolean matchStrings() {
		Matcher matcher = pattern.matcher(str);
		if (matcher.matches()) {
			if (uriTemplateVariables != null) {
				for (int i = 1; i <= matcher.groupCount(); i++) {
					String name = this.variableNames.get(i - 1);
					String value = matcher.group(i);
					uriTemplateVariables.put(name, value);
				}
			}
			return true;
		}
		else {
			return false;
		}
	}

}