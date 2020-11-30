/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube.http;

import io.activej.codec.registry.CodecFactory;
import io.activej.codec.registry.CodecRegistry;
import io.activej.common.exception.parse.ParseException;
import io.activej.cube.CubeQuery.Ordering;
import io.activej.cube.ReportType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static io.activej.aggregation.fieldtype.FieldTypes.LOCAL_DATE_CODEC;
import static java.util.stream.Collectors.toList;

class Utils {
	private static final ParseException MALFORMED_TAIL = new ParseException(Utils.class, "Tail is neither 'asc' nor 'desc'");
	private static final ParseException MISSING_SEMICOLON = new ParseException(Utils.class, "Failed to parse orderings, missing semicolon");

	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String WHERE_PARAM = "where";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";
	static final String REPORT_TYPE_PARAM = "reportType";

	private static final Pattern splitter = Pattern.compile(",");

	static String formatOrderings(List<Ordering> orderings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Ordering ordering : orderings) {
			sb.append(first ? "" : ",").append(ordering.getField()).append(":").append(ordering.isAsc() ? "ASC" : "DESC");
			first = false;
		}
		return sb.toString();
	}

	static List<Ordering> parseOrderings(String string) throws ParseException {
		List<Ordering> result = new ArrayList<>();
		List<String> tokens = splitter.splitAsStream(string)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
		for (String s : tokens) {
			int i = s.indexOf(':');
			if (i == -1) {
				throw MISSING_SEMICOLON;
			}
			String field = s.substring(0, i);
			String tail = s.substring(i + 1).toLowerCase();
			if ("asc".equals(tail))
				result.add(Ordering.asc(field));
			else if ("desc".equals(tail))
				result.add(Ordering.desc(field));
			else {
				throw MALFORMED_TAIL;
			}
		}
		return result;
	}

	static int parseNonNegativeInteger(String parameter) throws ParseException {
		try {
			int value = Integer.parseInt(parameter);
			if (value < 0 ) throw new ParseException("Must be non negative value: " + parameter);
			return value;
		} catch (NumberFormatException e) {
			throw new ParseException("Could not parse: " + parameter, e);
		}
	}

	static ReportType parseReportType(String parameter) throws ParseException{
		try {
			return ReportType.valueOf(parameter.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new ParseException("'" + parameter + "' neither of: " + Arrays.toString(ReportType.values()), e);
		}
	}

	public static final CodecFactory CUBE_TYPES = CodecRegistry.createDefault()
			.with(LocalDate.class, LOCAL_DATE_CODEC);
}
