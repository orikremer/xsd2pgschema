/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2017 Masashi Yokochi

    https://sourceforge.net/projects/xsd2pgschema/

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package net.sf.xsd2pgschema;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

/**
 * Enumerator of XSD data types and conversion functions.
 *
 * @author yokochi
 */
public enum XsDataType {

	/** The xs:boolean. */
	xs_boolean,
	/** The xs:hexBinary. */
	xs_hexBinary,
	/** The xs:base64Binary. */
	xs_base64Binary,
	/** The xs:bigserial. */
	xs_bigserial,
	/** The xs:serial. */
	xs_serial,

	/** The xs:float. */
	xs_float,
	/** The xs:double. */
	xs_double,
	/** The xs:decimal. */
	xs_decimal,
	/** The xs:long. */
	xs_long,
	/** The xs:bigint. */
	xs_bigint,
	/** The xs:integer. */
	xs_integer,
	/** The xs:int. */
	xs_int,
	/** The xs:short. */
	xs_short,
	/** The xs:byte. */
	xs_byte,

	/** The xs:nonPositiveInteger. */
	xs_nonPositiveInteger,
	/** The xs:negativeInteger. */
	xs_negativeInteger,
	/** The xs:nonNegativeInteger. */
	xs_nonNegativeInteger,
	/** The xs:positiveInteger. */
	xs_positiveInteger,

	/** The xs:unsignedLong. */
	xs_unsignedLong,
	/** The xs:unsignedInt. */
	xs_unsignedInt,
	/** The xs:unsignedShort. */
	xs_unsignedShort,
	/** The xs:unsignedByte. */
	xs_unsignedByte,

	/** The xs:duration. */
	xs_duration,
	/** The xs:dateTime. */
	xs_dateTime,
	/** The xs:time. */
	xs_time,
	/** The xs:date. */
	xs_date,
	/** The xs:gYearMonth. */
	xs_gYearMonth,
	/** The xs:gYear. */
	xs_gYear,
	/** The xs:gMonthDay. */
	xs_gMonthDay,
	/** The xs:gMonth. */
	xs_gMonth,
	/** The xs:gDay. */
	xs_gDay,

	/** The xs:string. */
	xs_string,
	/** The xs:normalizedString. */
	xs_normalizedString,
	/** The xs:token. */
	xs_token,
	/** The xs:language. */
	xs_language,
	/** The xs:Name. */
	xs_Name,
	/** The xs:QName. */
	xs_QName,
	/** The xs:NCName. */
	xs_NCName,
	/** The xs:anyURI. */
	xs_anyURI,

	/** The xs:NOTATION. */
	xs_NOTATION,
	/** The xs:NMTOKEN. */
	xs_NMTOKEN,
	/** The xs:NMTOKENS. */
	xs_NMTOKENS,
	/** The xs:ID. */
	xs_ID,
	/** The xs:IDREF. */
	xs_IDREF,
	/** The xs:IDREFS. */
	xs_IDREFS,
	/** The xs:ENTITY. */
	xs_ENTITY,
	/** The xs:ENTITIES. */
	xs_ENTITIES,

	/** The xs:anyType. */
	xs_anyType,
	/** The xs:any. */
	xs_any,
	/** The xs:anyAttribute. */
	xs_anyAttribute;

	/**
	 * Return least common data type.
	 *
	 * @param xs_type1 data type 1
	 * @param xs_type2 data type 2
	 * @return least common data type
	 */
	public static XsDataType getLeastCommonDataType(XsDataType xs_type1, XsDataType xs_type2) {

		if (xs_type1 == null || xs_type2 == null)
			return xs_anyType;

		if (xs_type1.equals(xs_type2))
			return xs_type1;

		switch (xs_type1) {
		case xs_boolean:
			return xs_anyType;
		case xs_hexBinary:
		case xs_base64Binary:
			switch (xs_type2) {
			case xs_hexBinary:
			case xs_base64Binary:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_bigserial:
			switch (xs_type2) {
			case xs_long:
			case xs_bigint:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_long;
			case xs_unsignedLong:
				return xs_type2;
			case xs_serial:
				return xs_bigserial;
			default:
				return xs_anyType;
			}
		case xs_long:
		case xs_bigint:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_unsignedLong:
			switch (xs_type2) {
			case xs_long:
			case xs_bigint:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_short:
			case xs_byte:
				return xs_long;
			case xs_bigserial:
			case xs_serial:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_serial:
		case xs_positiveInteger:
			switch (xs_type2) {
			case xs_bigserial:
				return xs_type2;
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_short:
			case xs_byte:
				return xs_int;
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_unsignedInt;
			case xs_serial:
			case xs_positiveInteger:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_integer:
		case xs_int:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_nonPositiveInteger:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_int;
			case xs_negativeInteger:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_negativeInteger:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_int;
			case xs_nonPositiveInteger:
				return xs_type2;
			default:
				return xs_anyType;
			}
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_positiveInteger:
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_short:
			case xs_byte:
				return xs_int;
			default:
				return xs_anyType;
			}
		case xs_float:
			switch (xs_type2) {
			case xs_double:
			case xs_decimal:
				return xs_type2;
			default:
				return xs_anyType;
			}
		case xs_double:
			switch (xs_type2) {
			case xs_float:
				return xs_type1;
			case xs_decimal:
				return xs_type2;
			default:
				return xs_anyType;
			}
		case xs_decimal:
			switch (xs_type2) {
			case xs_float:
			case xs_double:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_short:
		case xs_byte:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
				return xs_int;
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_unsignedShort:
		case xs_unsignedByte:
			switch (xs_type2) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				return xs_long;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
				return xs_int;
			case xs_short:
			case xs_byte:
				return xs_type2;
			case xs_unsignedShort:
			case xs_unsignedByte:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_dateTime:
			switch (xs_type2) {
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_date:
			switch (xs_type2) {
			case xs_dateTime:
				return xs_type2;
			case xs_gYearMonth:
			case xs_gYear:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_gYearMonth:
			switch (xs_type2) {
			case xs_dateTime:
			case xs_date:
				return xs_type2;
			case xs_gYear:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_gYear:
			switch (xs_type2) {
			case xs_dateTime:
			case xs_date:
			case xs_gYearMonth:
				return xs_type2;
			default:
				return xs_anyType;
			}
		case xs_gMonthDay:
			switch (xs_type2) {
			case xs_gMonth:
				return xs_type1;
			default:
				return xs_anyType;
			}
		case xs_gMonth:
			switch (xs_type2) {
			case xs_gMonthDay:
				return xs_type2;
			default:
				return xs_anyType;
			}
		case xs_time:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return xs_anyType;
		case xs_any:
			switch (xs_type2) {
			case xs_anyAttribute:
				return xs_any;
			default:
				return xs_anyType;
			}
		case xs_anyAttribute:
			switch (xs_type2) {
			case xs_any:
				return xs_any;
			default:
				return xs_anyType;
			}
		}

		return xs_anyType;
	}

	/**
	 * PostgreSQL DDL mapping.
	 *
	 * @param field PostgreSQL field
	 * @return String PostgreSQL DDL type definition
	 */
	public static String getPgDataType(PgField field) {

		String name = PgSchemaUtil.avoidPgReservedWords(field.name);

		String base;
		StringBuilder check = null;

		try {

			switch (field.xs_type) {
			case xs_boolean:
				return "BOOLEAN";
			case xs_hexBinary:
			case xs_base64Binary:
				return "BYTEA";
			case xs_bigserial:
				return "BIGSERIAL";
			case xs_serial:
				return "SERIAL";
			case xs_float:
				base = "REAL";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						float f = Float.parseFloat(field.min_inclusive);

						check.append(name + " >= " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						float f = Float.parseFloat(field.min_exclusive);

						check.append(name + " > " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						float f = Float.parseFloat(field.max_inclusive);

						check.append(name + " <= " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						float f = Float.parseFloat(field.max_exclusive);

						check.append(name + " < " + f + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_double:
				base = "DOUBLE PRECISION";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						double d = Double.parseDouble(field.min_inclusive);

						check.append(name + " >= " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						double d = Double.parseDouble(field.min_exclusive);

						check.append(name + " > " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						double d = Double.parseDouble(field.max_inclusive);

						check.append(name + " <= " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						double d = Double.parseDouble(field.max_exclusive);

						check.append(name + " < " + d + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_decimal:
				base = "DECIMAL";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(field.min_inclusive);

						check.append(name + " >= " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(field.min_exclusive);

						check.append(name + " > " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(field.max_inclusive);

						check.append(name + " <= " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						BigDecimal bd = new BigDecimal(field.max_exclusive);

						check.append(name + " < " + bd + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_integer:
			case xs_int:
				base = "INTEGER";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						int i = Integer.parseInt(field.min_inclusive);

						check.append(name + " >= " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						int i = Integer.parseInt(field.min_exclusive);

						check.append(name + " > " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						int i = Integer.parseInt(field.max_inclusive);

						check.append(name + " <= " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						int i = Integer.parseInt(field.max_exclusive);

						check.append(name + " < " + i + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.total_digits != null) {

					try {

						int i = Integer.parseInt(field.total_digits);

						if (i > 0)
							check.append(name + " < " + (int) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_long:
			case xs_bigint:
				base = "BIGINT";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						long l = Long.parseLong(field.min_inclusive);

						check.append(name + " >= " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						long l = Long.parseLong(field.min_exclusive);

						check.append(name + " > " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						long l = Long.parseLong(field.max_inclusive);

						check.append(name + " <= " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						long l = Long.parseLong(field.max_exclusive);

						check.append(name + " < " + l + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.total_digits != null) {

					try {

						int i = Integer.parseInt(field.total_digits);

						if (i > 0)
							check.append(name + " < " + (long) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_short:
			case xs_byte:
				base = "SMALLINT";

				if (!field.restriction)
					return base;

				check = new StringBuilder();

				if (field.min_inclusive != null) {

					try {

						short s = Short.parseShort(field.min_inclusive);

						check.append(name + " >= " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.min_exclusive != null) {

					try {

						short s = Short.parseShort(field.min_exclusive);

						check.append(name + " > " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.max_inclusive != null) {

					try {

						short s = Short.parseShort(field.max_inclusive);

						check.append(name + " <= " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				else if (field.max_exclusive != null) {

					try {

						short s = Short.parseShort(field.max_exclusive);

						check.append(name + " < " + s + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (field.total_digits != null) {

					try {

						int i = Integer.parseInt(field.total_digits);

						if (i > 0)
							check.append(name + " < " + (short) Math.pow(10, i) + " AND ");

					} catch (NumberFormatException e) {
					}

				}

				if (check.length() > 4)
					return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";

				return base;
			case xs_nonPositiveInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " <= 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							int i = Integer.parseInt(field.min_inclusive);

							check.append(name + " >= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.min_exclusive != null) {

						try {

							int i = Integer.parseInt(field.min_exclusive);

							check.append(name + " > " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.max_inclusive != null) {

						try {

							int i = Integer.parseInt(field.max_inclusive);

							if (i < 0)
								check.append(name + " <= " + i + " AND ");
							else
								check.append(name + " <= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " <= 0 AND ");
						}

					}

					else if (field.max_exclusive != null) {

						try {

							int i = Integer.parseInt(field.max_exclusive);

							if (i < 1)
								check.append(name + " < " + i + " AND ");
							else
								check.append(name + " <= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " <= 0 AND ");
						}

					}

					else
						check.append(name + " <= 0 AND ");

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_negativeInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " < 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							int i = Integer.parseInt(field.min_inclusive);

							check.append(name + " >= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.min_exclusive != null) {

						try {

							int i = Integer.parseInt(field.min_exclusive);

							check.append(name + " > " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.max_inclusive != null) {

						try {

							int i = Integer.parseInt(field.max_inclusive);

							if (i < -1)
								check.append(name + " <= " + i + " AND ");
							else
								check.append(name + " < 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " < 0 AND ");
						}

					}

					else if (field.max_exclusive != null) {

						try {

							int i = Integer.parseInt(field.max_exclusive);

							if (i < 0)
								check.append(name + " < " + i + " AND ");
							else
								check.append(name + " < 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " < 0 AND ");
						}

					}

					else
						check.append(name + " < 0 AND ");

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
				base = "INTEGER";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " >= 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							int i = Integer.parseInt(field.min_inclusive);

							if (i > 0)
								check.append(name + " >= " + i + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else if (field.min_exclusive != null) {

						try {

							int i = Integer.parseInt(field.min_exclusive);

							if (i > -1)
								check.append(name + " > " + i + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else
						check.append(name + " >= 0 AND ");

					if (field.max_inclusive != null) {

						try {

							int i = Integer.parseInt(field.max_inclusive);

							check.append(name + " <= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.max_exclusive != null) {

						try {

							int i = Integer.parseInt(field.max_exclusive);

							check.append(name + " < " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.total_digits != null) {

						try {

							int i = Integer.parseInt(field.total_digits);

							if (i > 0)
								check.append(name + " < " + (int) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_positiveInteger:
				base = "INTEGER";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " > 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							int i = Integer.parseInt(field.min_inclusive);

							if (i > 1)
								check.append(name + " >= " + i + " AND ");
							else
								check.append(name + " > 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " > 0 AND ");
						}

					}

					else if (field.min_exclusive != null) {

						try {

							int i = Integer.parseInt(field.min_exclusive);

							if (i > 0)
								check.append(name + " > " + i + " AND ");
							else
								check.append(name + " > 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " > 0 AND ");
						}

					}

					else
						check.append(name + " > 0 AND ");

					if (field.max_inclusive != null) {

						try {

							int i = Integer.parseInt(field.max_inclusive);

							check.append(name + " <= " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.max_exclusive != null) {

						try {

							int i = Integer.parseInt(field.max_exclusive);

							check.append(name + " < " + i + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.total_digits != null) {

						try {

							int i = Integer.parseInt(field.total_digits);

							if (i > 0)
								check.append(name + " < " + (int) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_unsignedLong:
				base = "BIGINT";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " >= 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							long l = Long.parseLong(field.min_inclusive);

							if (l > 0)
								check.append(name + " >= " + l + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else if (field.min_exclusive != null) {

						try {

							long l = Long.parseLong(field.min_exclusive);

							if (l > -1)
								check.append(name + " > " + l + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else
						check.append(name + " >= 0 AND ");

					if (field.max_inclusive != null) {

						try {

							long l = Long.parseLong(field.max_inclusive);

							check.append(name + " <= " + l + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.max_exclusive != null) {

						try {

							long l = Long.parseLong(field.max_exclusive);

							check.append(name + " < " + l + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.total_digits != null) {

						try {

							int i = Integer.parseInt(field.total_digits);

							if (i > 0)
								check.append(name + " < " + (long) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_unsignedShort:
			case xs_unsignedByte:
				base = "SMALLINT";

				check = new StringBuilder();

				if (!field.restriction)
					check.append(name + " >= 0 AND ");

				else {

					if (field.min_inclusive != null) {

						try {

							short s = Short.parseShort(field.min_inclusive);

							if (s > 0)
								check.append(name + " >= " + s + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else if (field.min_exclusive != null) {

						try {

							short s = Short.parseShort(field.min_exclusive);

							if (s > -1)
								check.append(name + " > " + s + " AND ");
							else
								check.append(name + " >= 0 AND ");

						} catch (NumberFormatException e) {
							check.append(name + " >= 0 AND ");
						}

					}

					else
						check.append(name + " >= 0 AND ");

					if (field.max_inclusive != null) {

						try {

							short s = Short.parseShort(field.max_inclusive);

							check.append(name + " <= " + s + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					else if (field.max_exclusive != null) {

						try {

							short s = Short.parseShort(field.max_exclusive);

							check.append(name + " < " + s + " AND ");

						} catch (NumberFormatException e) {
						}

					}

					if (field.total_digits != null) {

						try {

							int i = Integer.parseInt(field.total_digits);

							if (i > 0)
								check.append(name + " < " + (short) Math.pow(10, i) + " AND ");

						} catch (NumberFormatException e) {
						}

					}

				}

				return base + " CHECK ( " + check.substring(0, check.length() - 4) + ")";
			case xs_dateTime:
				if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
					return "TIMESTAMP";
				else
					return "TIMESTAMP WITH TIME ZONE";
			case xs_time:
				if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
					return "TIME";
				else
					return "TIME WITH TIME ZONE";
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				return "DATE";
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
			case xs_duration:
			case xs_anyType:
			case xs_string:
			case xs_normalizedString:
			case xs_token:
			case xs_NMTOKEN:
			case xs_NMTOKENS:
			case xs_IDREFS:
			case xs_ENTITIES:
			case xs_NOTATION:
			case xs_language:
			case xs_Name:
			case xs_QName:
			case xs_NCName:
			case xs_anyURI:
			case xs_ID:
			case xs_IDREF:
			case xs_ENTITY:
				if (field._list) // length restriction of xs:list is not effective
					return "TEXT";

				if (field.length != null) {

					try {

						int i = Integer.parseInt(field.length);

						if (i > 0)
							return "VARCHAR(" + i + ")";
						else
							return "TEXT";

					} catch (NumberFormatException e) {
						return "TEXT";
					}

				}

				if (field.max_length != null) {

					try {

						int i = Integer.parseInt(field.max_length);

						if (i > 0)
							return "VARCHAR(" + i + ")";
						else
							return "TEXT";

					} catch (NumberFormatException e) {
						return "TEXT";
					}

				}

				return "TEXT";
			case xs_any:
			case xs_anyAttribute:
				return "XML";
			}

		} finally {
			if (check != null) {
				check.setLength(0);
				check = null;
			}
		}

		return null;
	}

	/**
	 * JSON Schema type mapping.
	 *
	 * @param field PostgreSQL field
	 * @return String JSON Schema type definition
	 */
	public static String getJsonSchemaType(PgField field) {

		switch (field.xs_type) {
		case xs_boolean:
			return "\"boolean\"";
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_int:
		case xs_long:
		case xs_bigint:
		case xs_short:
		case xs_byte:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
		case xs_positiveInteger:
		case xs_unsignedLong:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return "\"number\"";
		case xs_hexBinary:
		case xs_base64Binary:
		case xs_dateTime:
		case xs_time:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
		case xs_any:
		case xs_anyAttribute:
			return "\"string\"";
		}

		return "\"null\"";
	}

	/**
	 * JSON Schema $ref mapping.
	 *
	 * @param field PostgreSQL field
	 * @return String JSON Schema $ref definition
	 */
	public static String getJsonSchemaRef(PgField field) {
		return "http://www.jsonix.org/jsonschemas/w3c/2001/XMLSchema.jsonschema#/definitions/" + field.xs_type.toString().replaceFirst("^xs_", "");
	}

	/**
	 * Return JSON Schema default value.
	 *
	 * @param field PostgreSQL field
	 * @return String JSON Schema default value
	 */
	public static String getJsonSchemaDefaultValue(PgField field) {

		switch (field.xs_type) {
		case xs_boolean:
			return field.default_value;
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_int:
		case xs_long:
		case xs_bigint:
		case xs_short:
		case xs_byte:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
		case xs_positiveInteger:
		case xs_unsignedLong:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return field.default_value;
		case xs_hexBinary:
		case xs_base64Binary:
		case xs_dateTime:
		case xs_time:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return "\"" + field.default_value + "\"";
		default:
		}

		return "null";
	}

	/**
	 * Return JSON Schema enumeration array.
	 *
	 * @param field PostgreSQL field
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema enumeration array
	 */
	public static String getJsonSchemaEnumArray(PgField field, String json_key_value_space) {

		StringBuilder sb = new StringBuilder();

		try {

			switch (field.xs_type) {
			case xs_boolean:
			case xs_bigserial:
			case xs_serial:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_integer:
			case xs_int:
			case xs_long:
			case xs_bigint:
			case xs_short:
			case xs_byte:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_unsignedInt:
			case xs_positiveInteger:
			case xs_unsignedLong:
			case xs_unsignedShort:
			case xs_unsignedByte:
				for (String enumeration : field.xenumeration)
					sb.append(enumeration + "," + json_key_value_space);
				break;
			case xs_hexBinary:
			case xs_base64Binary:
			case xs_dateTime:
			case xs_time:
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
			case xs_duration:
			case xs_anyType:
			case xs_string:
			case xs_normalizedString:
			case xs_token:
			case xs_NMTOKEN:
			case xs_NMTOKENS:
			case xs_IDREFS:
			case xs_ENTITIES:
			case xs_NOTATION:
			case xs_language:
			case xs_Name:
			case xs_QName:
			case xs_NCName:
			case xs_anyURI:
			case xs_ID:
			case xs_IDREF:
			case xs_ENTITY:
				for (String enumeration : field.xenumeration)
					sb.append("\"" + enumeration + "\"," + json_key_value_space);
				break;
			default:
			}

			return sb.toString();

		} finally {
			sb.setLength(0);
			sb = null;
		}
	}

	/**
	 * Return JSON Schema maximum value.
	 *
	 * @param field PostgreSQL field
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema maximum value
	 */
	public static String getJsonSchemaMaximumValue(PgField field, String json_key_value_space) {

		switch (field.xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_int:
		case xs_long:
		case xs_bigint:
		case xs_short:
		case xs_byte:
			if (field.max_inclusive != null)
				return field.max_inclusive;
			else if (field.max_exclusive != null)
				return field.max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
			break;
		case xs_nonPositiveInteger:
			if (!field.restriction)
				return "0";

			if (field.max_inclusive != null) {

				try {

					int i = Integer.parseInt(field.max_inclusive);

					if (i < 0)
						return field.max_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (field.max_exclusive != null) {

				try {

					int i = Integer.parseInt(field.max_exclusive);

					if (i < 1)
						return i + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_negativeInteger:
			if (!field.restriction)
				return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

			if (field.max_inclusive != null) {

				try {

					int i = Integer.parseInt(field.max_inclusive);

					if (i < -1)
						return field.max_inclusive;
					else
						return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
				}

			}

			else if (field.max_exclusive != null) {

				try {

					int i = Integer.parseInt(field.max_exclusive);

					if (i < 0)
						return i + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
					else
						return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
				}

			}

			else
				return "0," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!field.restriction)
				return null;

			if (field.max_inclusive != null) {

				try {

					Integer.parseInt(field.max_inclusive);

					return field.max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.max_exclusive != null) {

				try {

					Integer.parseInt(field.max_exclusive);

					return field.max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (field.total_digits != null) {

				try {

					int i = Integer.parseInt(field.total_digits);

					if (i > 0)
						return String.valueOf((int) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_positiveInteger:
			if (!field.restriction)
				return null;

			if (field.max_inclusive != null) {

				try {

					Integer.parseInt(field.max_inclusive);

					return field.max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.max_exclusive != null) {

				try {

					Integer.parseInt(field.max_exclusive);

					return field.max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (field.total_digits != null) {

				try {

					int i = Integer.parseInt(field.total_digits);

					if (i > 0)
						return String.valueOf((int) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedLong:
			if (!field.restriction)
				return null;

			if (field.max_inclusive != null) {

				try {

					Long.parseLong(field.max_inclusive);

					return field.max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.max_exclusive != null) {

				try {

					Long.parseLong(field.max_exclusive);

					return field.max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (field.total_digits != null) {

				try {

					int i = Integer.parseInt(field.total_digits);

					if (i > 0)
						return String.valueOf((long) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!field.restriction)
				return null;

			if (field.max_inclusive != null) {

				try {

					Short.parseShort(field.max_inclusive);

					return field.max_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.max_exclusive != null) {

				try {

					Short.parseShort(field.max_exclusive);

					return field.max_exclusive + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}

			if (field.total_digits != null) {

				try {

					int i = Integer.parseInt(field.total_digits);

					if (i > 0)
						return String.valueOf((short) Math.pow(10, i)) + "," + json_key_value_space + "\"exclusiveMaximum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		default:
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema minimum value.
	 *
	 * @param field PostgreSQL field
	 * @param json_key_value_space the JSON key value space
	 * @return String JSON Schema minimum value
	 */
	public static String getJsonSchemaMinimumValue(PgField field, String json_key_value_space) {

		switch (field.xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_int:
		case xs_long:
		case xs_bigint:
		case xs_short:
		case xs_byte:
			if (field.min_inclusive != null)
				return field.min_inclusive;
			else if (field.min_exclusive != null)
				return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
			break;
		case xs_nonPositiveInteger:
			if (!field.restriction)
				return null;

			if (field.min_inclusive != null) {

				try {

					Integer.parseInt(field.min_inclusive);

					return field.min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.min_exclusive != null) {

				try {

					Integer.parseInt(field.min_exclusive);

					return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_negativeInteger:
			if (!field.restriction)
				return null;

			if (field.min_inclusive != null) {

				try {

					Integer.parseInt(field.min_inclusive);

					return field.min_inclusive;

				} catch (NumberFormatException e) {
				}

			}

			else if (field.min_exclusive != null) {

				try {

					Integer.parseInt(field.min_exclusive);

					return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
				}

			}
			break;
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			if (!field.restriction)
				return "0";

			if (field.min_inclusive != null) {

				try {

					int i = Integer.parseInt(field.min_inclusive);

					if (i > 0)
						return field.min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (field.min_exclusive != null) {

				try {

					int i = Integer.parseInt(field.min_exclusive);

					if (i > -1)
						return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_positiveInteger:
			if (!field.restriction)
				return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

			if (field.min_inclusive != null) {

				try {

					int i = Integer.parseInt(field.min_inclusive);

					if (i > 1)
						return field.min_inclusive;
					else
						return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
				}

			}

			else if (field.min_exclusive != null) {

				try {

					int i = Integer.parseInt(field.min_exclusive);

					if (i > 0)
						return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";

				} catch (NumberFormatException e) {
					return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
				}

			}

			else
				return "0," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
		case xs_unsignedLong:
			if (!field.restriction)
				return "0";

			if (field.min_inclusive != null) {

				try {

					long l = Long.parseLong(field.min_inclusive);

					if (l > 0)
						return field.min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (field.min_exclusive != null) {

				try {

					long l = Long.parseLong(field.min_exclusive);

					if (l > -1)
						return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (!field.restriction)
				return "0";

			if (field.min_inclusive != null) {

				try {

					short s = Short.parseShort(field.min_inclusive);

					if (s > 0)
						return field.min_inclusive;
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else if (field.min_exclusive != null) {

				try {

					short s = Short.parseShort(field.min_exclusive);

					if (s > -1)
						return field.min_exclusive + "," + json_key_value_space + "\"exclusiveMinimum\":" + json_key_value_space + "true";
					else
						return "0";

				} catch (NumberFormatException e) {
					return "0";
				}

			}

			else
				return "0";
		default:
			return	null;
		}

		return null;
	}

	/**
	 * Return JSON Schema multipleOf value.
	 *
	 * @param field PostgreSQL field
	 * @return String JSON Schema multipleOf value
	 */
	public static String getJsonSchemaMultipleOfValue(PgField field) {

		switch (field.xs_type) {
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (field.fraction_digits == null)
				return null;

			try {

				int i = Integer.parseInt(field.total_digits);

				if (i > 0) {

					StringBuilder sb = new StringBuilder();

					try {

						sb.append("0.");

						for (int j = 1; j < i; j++)
							sb.append("0");

						sb.append("1");

						return sb.toString();

					} finally {
						sb.setLength(0);
						sb = null;
					}

				}

			} catch (NumberFormatException e) {
				return null;
			}
			break;
		case xs_bigserial:
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_long:
		case xs_bigint:
		case xs_short:
		case xs_byte:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
		case xs_positiveInteger:
		case xs_unsignedLong:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return "1";
		default:
			return	null;
		}

		return null;
	}

	/**
	 * Validate data.
	 *
	 * @param field PostgreSQL field
	 * @param value content
	 * @return boolean whether content is valid
	 */
	public static boolean isValid(PgField field, String value) {

		if (field.restriction && field.min_length != null) {

			if (value.length() < Integer.valueOf(field.min_length))
				return false;

		}

		switch (field.xs_type) {
		case xs_boolean:
			return true;
		case xs_bigserial:
			try {
				Long l = Long.parseLong(value);
				return (l > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_serial:
			try {
				Integer i = Integer.parseInt(value);
				return (i > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_hexBinary:
			try {
				DatatypeConverter.parseHexBinary(value);
			} catch (IllegalArgumentException e) {
				return false;
			}
		case xs_base64Binary:
			try {
				DatatypeConverter.parseBase64Binary(value);
			} catch (IllegalArgumentException e) {
				return false;
			}
		case xs_long:
		case xs_bigint:
			try {
				Long.parseLong(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_float:
			try {
				Float.parseFloat(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_double:
			try {
				Double.parseDouble(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_decimal:
			try {
				new BigDecimal(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_integer:
		case xs_int:
			try {
				Integer.parseInt(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_short:
		case xs_byte:
			try {
				Short.parseShort(value);
			} catch (NumberFormatException e) {
				return false;
			}
			return true;
		case xs_nonPositiveInteger:
			try {
				int i = Integer.parseInt(value);
				return (i <= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_negativeInteger:
			try {
				int i = Integer.parseInt(value);
				return (i < 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			try {
				int i = Integer.parseInt(value);
				return (i >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_positiveInteger:
			try {
				int i = Integer.parseInt(value);
				return (i > 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedLong:
			try {
				long l = Long.parseLong(value);
				return (l >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_unsignedShort:
		case xs_unsignedByte:
			try {
				short s = Short.parseShort(value);
				return (s >= 0);
			} catch (NumberFormatException e) {
				return false;
			}
		case xs_dateTime:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
			return (parseDate(value) != null);
		case xs_time:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required"))) {

				try {
					LocalTime.parse(value);
				} catch (DateTimeParseException e) {

					try {
						OffsetTime.parse(value);
					} catch (DateTimeParseException e2) {
						return false;
					}
				}

			}
			else {

				try {
					OffsetTime.parse(value);
				} catch (DateTimeParseException e) {

					try {
						LocalTime.parse(value);
					} catch (DateTimeParseException e2) {
						return false;
					}

				}

			}
			return true;
		default:
			return true;
		}

	}

	/**
	 * Return normalized value.
	 *
	 * @param field PostgreSQL field
	 * @param value content
	 * @return String normalized format
	 */
	public static String normalize(PgField field, String value) {

		switch (field.xs_type) {
		case xs_hexBinary:
			return "E'\\\\x" + value + "'";
		case xs_base64Binary:
			return "decode('" + value + "','base64')";
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			Calendar cal = Calendar.getInstance();

			cal.setTime(parseDate(value));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

			return format.format(cal.getTime());
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (!field.restriction)
				return value;

			// xs:fractionDigits

			if (field.fraction_digits != null) {

				Integer i = Integer.parseInt(field.fraction_digits);

				if (i < 0)
					return value;

				BigDecimal b = new BigDecimal(value);
				b.setScale(i);

				return b.toString();
			}

			return value;
		case xs_any:
		case xs_anyAttribute:
			return "XMLPARSE (CONTENT '" + value + "')";
		default:
			if (!field.restriction)
				return value;

			// xs:whiteSpace

			if (field.white_space != null) {

				if (field.white_space.equals("replace"))
					value = value.replaceAll("[\t\n\r]", " ");

				else if (field.white_space.equals("collapse"))
					value = value.trim().replaceAll("[\t\n\r]", " ").replaceAll("\\s+", " ");

			}

			return value;
		}

	}

	/**
	 * java.sql.Types mapping.
	 *
	 * @param field PostgreSQL field
	 * @return int java.sqlTypes
	 */
	public static int getSqlDataType(PgField field) {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		switch (xs_type) {
		case xs_boolean:
			return java.sql.Types.BOOLEAN;
		case xs_hexBinary:
		case xs_base64Binary:
			return java.sql.Types.BINARY;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			return java.sql.Types.BIGINT;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			return java.sql.Types.INTEGER;
		case xs_float:
			return java.sql.Types.FLOAT;
		case xs_double:
			return java.sql.Types.DOUBLE;
		case xs_decimal:
			return java.sql.Types.DECIMAL;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return java.sql.Types.SMALLINT;
		case xs_dateTime:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
				return java.sql.Types.TIMESTAMP;
			else
				return java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
		case xs_time:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
				return java.sql.Types.TIME;
			else
				return java.sql.Types.TIME_WITH_TIMEZONE;
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			return java.sql.Types.DATE;
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			return java.sql.Types.VARCHAR;
		case xs_any:
		case xs_anyAttribute:
			return java.sql.Types.SQLXML;
		}

		return java.sql.Types.NULL;
	}

	/**
	 * Return SQL predicate of given value.
	 *
	 * @param field PostgreSQL field
	 * @param value content
	 * @return String SQL predicate
	 */
	public static String getSqlPredicate(PgField field, String value) {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		switch (xs_type) {
		case xs_hexBinary:
			return "E'\\\\x" + value + "'";
		case xs_base64Binary:
			return "decode('" + value + "','base64')";
		case xs_boolean:
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return value;
		case xs_dateTime:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
				return "TIMESTAMP '" + value + "'";
			else
				return "TIMESTAMP WITH TIME ZONE '" + value + "'";
		case xs_time:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
				return "TIME '" + value + "'";
			else
				return "TIME WITH TIME ZONE '" + value + "'";
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			return "DATE '" + value + "'";
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			if (field.enum_name == null)
				return "'" + value.replaceAll("'", "''") + "'";
			else {
				if (value.length() > PgSchemaUtil.max_enum_len)
					value = value.substring(0, PgSchemaUtil.max_enum_len);
				return "'" + value.replaceAll("'", "''") + "'";
			}
		case xs_any:
		case xs_anyAttribute:
			return "XMLPARSE (CONTENT '" + value + "')";
		}

		return null;
	}

	/**
	 * Set value via PreparedStatement.
	 *
	 * @param field PostgreSQL field
	 * @param ps prepared statement
	 * @param par_idx parameter index id
	 * @param value content
	 * @throws SQLException the SQL exception
	 */
	public static void setValue(PgField field, PreparedStatement ps, int par_idx, String value) throws SQLException {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		switch (xs_type) {
		case xs_boolean:
			ps.setBoolean(par_idx, Boolean.valueOf(value));
			break;
		case xs_hexBinary:
			ps.setBytes(par_idx, DatatypeConverter.parseHexBinary(value));
			break;
		case xs_base64Binary:
			ps.setBytes(par_idx, DatatypeConverter.parseBase64Binary(value));
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			try {
				ps.setLong(par_idx, Long.valueOf(value));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			ps.setInt(par_idx, Integer.valueOf(value));
			break;
		case xs_float:
			ps.setFloat(par_idx, Float.valueOf(value));
			break;
		case xs_double:
			ps.setDouble(par_idx, Double.valueOf(value));
			break;
		case xs_decimal:
			ps.setBigDecimal(par_idx, new BigDecimal(value));
			break;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			ps.setInt(par_idx, Integer.valueOf(value));
			break;
		case xs_dateTime:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required")))
				ps.setTimestamp(par_idx, new java.sql.Timestamp(parseDate(value).getTime()));
			else
				ps.setTimestamp(par_idx, new java.sql.Timestamp(parseDate(value).getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
			break;
		case xs_time:
			if (!field.restriction || (field.explicit_timezone != null && !field.explicit_timezone.equals("required"))) {

				try {
					ps.setTime(par_idx, java.sql.Time.valueOf(LocalTime.parse(value)));
				} catch (DateTimeParseException e) {

					try {
						ps.setTime(par_idx, java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime()));
					} catch (DateTimeParseException e2) {
					}
				}

			}

			else {

				try {
					ps.setTime(par_idx, java.sql.Time.valueOf(OffsetTime.parse(value).toLocalTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
				} catch (DateTimeParseException e) {

					try {
						ps.setTime(par_idx, java.sql.Time.valueOf(LocalTime.parse(value)), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
					} catch (DateTimeParseException e2) {
					}
				}

			}
			break;
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			Calendar cal = Calendar.getInstance();

			cal.setTime(parseDate(value));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			ps.setDate(par_idx, new java.sql.Date(cal.getTimeInMillis()));
			break;
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_duration:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_IDREFS:
		case xs_ENTITIES:
		case xs_NOTATION:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_ID:
		case xs_IDREF:
		case xs_ENTITY:
			ps.setString(par_idx, value);
			break;
		default: // xs_any, xs_anyAttribute
		}

	}

	/**
	 * Set XML object via PreparedStatement.
	 *
	 * @param field the field
	 * @param ps prepared statement
	 * @param par_idx parameter index id
	 * @param value content
	 * @throws SQLException the SQL exception
	 */
	public static void setValue(PgField field, PreparedStatement ps, int par_idx, SQLXML value) throws SQLException {

		switch (field.xs_type) {
		case xs_any:
		case xs_anyAttribute:
			ps.setSQLXML(par_idx, value);
		default: // xs_anyType
		}

	}

	// Lucene full-text indexing

	/**
	 * Set value to Lucene document.
	 *
	 * @param field PostgreSQL field
	 * @param lucene_doc Lucene document
	 * @param name field name
	 * @param value content
	 * @param min_word_len_filter whether it exceeds minimum word length
	 * @param numeric_index whether numeric values are stored in index
	 */
	public static void setValue(PgField field, org.apache.lucene.document.Document lucene_doc, String name, String value, boolean min_word_len_filter, boolean numeric_index) {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		switch (xs_type) {
		case xs_boolean:
		case xs_hexBinary:
		case xs_base64Binary:
			if (field.attr_sel)
				lucene_doc.add(new StringField(name, value, Field.Store.YES));
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			if (field.attr_sel) {
				lucene_doc.add(new LongPoint(name, Long.valueOf(value)));
				if (numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
			}
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			if (field.attr_sel) {
				lucene_doc.add(new IntPoint(name, Integer.valueOf(value)));
				if (numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
			}
			break;
		case xs_float:
			if (field.attr_sel) {
				lucene_doc.add(new FloatPoint(name, Float.valueOf(value)));
				if (numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
			}
			break;
		case xs_double:
		case xs_decimal:
			if (field.attr_sel) {
				lucene_doc.add(new DoublePoint(name, Double.valueOf(value)));
				if (numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
			}
			break;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (field.attr_sel) {
				lucene_doc.add(new IntPoint(name, Integer.valueOf(value)));
				if (numeric_index)
					lucene_doc.add(new StringField(name, value, Field.Store.YES));
			}
			break;
		case xs_dateTime:
			if (field.attr_sel) {
				java.util.Date util_time = parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_time, DateTools.Resolution.SECOND), Field.Store.YES));
			}
			break;
		case xs_date:
			if (field.attr_sel) {
				java.util.Date util_date = parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_date, DateTools.Resolution.DAY), Field.Store.YES));
			}
			break;
		case xs_gYearMonth:
			if (field.attr_sel) {
				java.util.Date util_month = parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_month, DateTools.Resolution.MONTH), Field.Store.YES));
			}
			break;
		case xs_gYear:
			if (field.attr_sel) {
				java.util.Date util_year = parseDate(value);
				lucene_doc.add(new StringField(name, DateTools.dateToString(util_year, DateTools.Resolution.YEAR), Field.Store.YES));
			}
			break;
		case xs_time:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
			if (field.attr_sel)
				lucene_doc.add(new StringField(name, value, Field.Store.YES));
			break;
		default:
			if (field.attr_sel || xs_type.equals(xs_ID))
				lucene_doc.add(new TextField(name, value, Field.Store.YES));
		}

		switch (xs_type) {
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			lucene_doc.add(new VecTextField(PgSchemaUtil.simple_content_name, value, Field.Store.NO));
			break;
		default:
			if (min_word_len_filter)
				lucene_doc.add(new VecTextField(PgSchemaUtil.simple_content_name, value, Field.Store.NO));
		}

	}

	/**
	 * Set system key.
	 *
	 * @param lucene_doc Lucene document
	 * @param name field name
	 * @param value content
	 */
	public static void setKey(org.apache.lucene.document.Document lucene_doc, String name, String value) {

		lucene_doc.add(new NoIdxStringField(name, value, Field.Store.YES));

	}

	// Sphinx full-text indexing

	/**
	 * Set value to Sphinx data source.
	 *
	 * @param field PostgreSQL field
	 * @param writer the writer
	 * @param attr_name Sphinx attribute name
	 * @param value content
	 * @param min_word_len_filter whether it exceeds minimum word length
	 */
	public static void setValue(PgField field, FileWriter writer, String attr_name, String value, boolean min_word_len_filter) {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		try {

			switch (xs_type) {
			case xs_boolean:
			case xs_hexBinary:
			case xs_base64Binary:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					writer.write("<" + attr_name + ">" + StringEscapeUtils.escapeXml10(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (!field.sph_mva) {
						if (++field.sph_attr_occurs > 1)
							field.sph_mva = true;
					}
					writer.write("<" + attr_name + ">" + Long.valueOf(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (!field.sph_mva) {
						if (++field.sph_attr_occurs > 1)
							field.sph_mva = true;
					}
					writer.write("<" + attr_name + ">" + Integer.valueOf(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_float:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					writer.write("<" + attr_name + ">" + Float.valueOf(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_double:
			case xs_decimal:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					writer.write("<" + attr_name + ">" + Double.valueOf(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (!field.sph_mva) {
						if (++field.sph_attr_occurs > 1)
							field.sph_mva = true;
					}
					writer.write("<" + attr_name + ">" + Integer.valueOf(value) + "</" + attr_name + ">\n");
				}
				break;
			case xs_dateTime:
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					java.util.Date util_time = parseDate(value);
					writer.write("<" + attr_name + ">" + util_time.getTime() / 1000L + "</" + attr_name + ">\n");
				}
				break;
			case xs_time:
			case xs_gMonthDay:
			case xs_gMonth:
			case xs_gDay:
				if (field.attr_sel) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					writer.write("<" + attr_name + ">" + StringEscapeUtils.escapeXml10(value) + "</" + attr_name + ">\n");
				}
				break;
			default:
				value = StringEscapeUtils.escapeXml10(value);
				if (field.attr_sel || xs_type.equals(xs_ID)) {
					if (!field.sph_attr)
						field.sph_attr = true;
					if (++field.sph_attr_occurs > 1)
						return;
					writer.write("<" + attr_name + ">" + value + "</" + attr_name + ">\n");
				}
			}

			switch (xs_type) {
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				writer.write("<" + PgSchemaUtil.simple_content_name + ">" + value + "</" + PgSchemaUtil.simple_content_name + ">\n");
				break;
			default:
				if (min_word_len_filter)
					writer.write("<" + PgSchemaUtil.simple_content_name + ">" + value + "</" + PgSchemaUtil.simple_content_name + ">\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// JSON array object

	/**
	 * Set value in field's JSON buffer.
	 *
	 * @param field PostgreSQL field
	 * @param value content
	 * @param json_key_value_space while spaces between values
	 * @return true, if successful
	 */
	public static boolean setValue(PgField field, String value, String json_key_value_space) {

		if (field.jsonb == null)
			return false;

		field.jsonb_col_size++;

		if (value == null || value.isEmpty()) {

			if (++field.jsonb_null_size > 100 && field.jsonb_col_size == field.jsonb_null_size) {

				field.jsonb.setLength(0);
				field.jsonb = null;

				field.jsonb_col_size = field.jsonb_null_size = 0;

				return false;
			}

			switch (field.xs_type) {
			case xs_boolean:
			case xs_bigserial:
			case xs_long:
			case xs_bigint:
			case xs_unsignedLong:
			case xs_serial:
			case xs_integer:
			case xs_int:
			case xs_nonPositiveInteger:
			case xs_negativeInteger:
			case xs_nonNegativeInteger:
			case xs_positiveInteger:
			case xs_unsignedInt:
			case xs_float:
			case xs_double:
			case xs_decimal:
			case xs_short:
			case xs_byte:
			case xs_unsignedShort:
			case xs_unsignedByte:
				field.jsonb.append("null");
				break;
			default:
				field.jsonb.append(value == null ? "null" : "\"\"");
			}

			field.jsonb.append("," + json_key_value_space);

			return false;
		}

		if (!field.jsonb_not_empty)
			field.jsonb_not_empty = true;

		value = StringEscapeUtils.escapeEcmaScript(value).replaceAll("\\\\/", "/").replaceAll("\\\\'", "'");

		switch (field.xs_type) {
		case xs_boolean:
			field.jsonb.append(value);
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
			field.jsonb.append(Long.parseLong(value));
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
			field.jsonb.append(Integer.parseInt(value));
			break;
		case xs_float:
			field.jsonb.append(Float.parseFloat(value));
			break;
		case xs_double:
			field.jsonb.append(Double.parseDouble(value));
			break;
		case xs_decimal:
			field.jsonb.append(new BigDecimal(value));
			break;
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			field.jsonb.append(Integer.parseInt(value));
			break;
		default:
			value = StringEscapeUtils.escapeCsv(value);
			if (value.startsWith("\""))
				field.jsonb.append(value);
			else
				field.jsonb.append("\"" + value + "\"");
		}

		field.jsonb.append("," + json_key_value_space);

		return true;
	}

	// Type dependent attribute selection of full-text indexing

	/**
	 * Append field as attribute of index.
	 *
	 * @param table PosgtreSQL table
	 * @param field PostgreSQL field
	 * @param index_filter index filter
	 */
	public static void appendAttr(PgTable table, PgField field, IndexFilter index_filter) {

		XsDataType xs_type = field.enum_name == null ? field.xs_type : xs_string;

		switch (xs_type) {
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_byte:
		case xs_unsignedShort:
		case xs_unsignedByte:
			if (index_filter.attr_integer)
				index_filter.addAttr(table.name + "." + field.name);
			break;
		case xs_float:
		case xs_double:
		case xs_decimal:
			if (index_filter.attr_float)
				index_filter.addAttr(table.name + "." + field.name);
			break;
		case xs_dateTime:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			if (index_filter.attr_date)
				index_filter.addAttr(table.name + "." + field.name);
			break;
		case xs_time:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
			if (index_filter.attr_time)
				index_filter.addAttr(table.name + "." + field.name);
			break;
		default:
			if (index_filter.attr_string)
				index_filter.addAttr(table.name + "." + field.name);
		}

	}

	// Sphinx attribute

	/**
	 * Write Sphinx attribute in schema file
	 * 
	 * @param table PosgtreSQL table
	 * @param field PostgreSQL field
	 * @param filew Sphinx schema file writer
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeSphSchemaAttr(PgTable table, PgField field, FileWriter filew) throws IOException {

		filew.write("<sphinx:attr name=\"" + table.name + "__" + field.xname + "\"");

		String attrs = null;

		switch (field.xs_type) {
		case xs_boolean:
			attrs = " type=\"bool\"";
			break;
		case xs_hexBinary:
		case xs_base64Binary:
			attrs = " type=\"string\"";
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_duration:
			attrs = " type=\"bigint\"";
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
			attrs = " type=\"int\" bits=\"32\"";
			break;
		case xs_unsignedInt:
			attrs = " type=\"int\" bits=\"32\"";
			break;
		case xs_float:
		case xs_double:
		case xs_decimal:
			attrs = " type=\"float\"";
			break;
		case xs_short:
			attrs = " type=\"int\" bits=\"16\"";
			break;
		case xs_unsignedShort:
			attrs = " type=\"int\" bits=\"16\"";
			break;
		case xs_byte:
			attrs = " type=\"int\" bits=\"8\"";
			break;
		case xs_unsignedByte:
			attrs = " type=\"int\" bits=\"8\"";
			break;
		case xs_dateTime:
		case xs_time:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			attrs = " type=\"timestamp\"";
			break;
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_NOTATION:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_ID:
		case xs_IDREF:
		case xs_IDREFS:
		case xs_ENTITY:
		case xs_ENTITIES:
		case xs_any:
		case xs_anyAttribute:
			attrs = " type=\"string\"";
			break;
		}

		if (field.sph_mva) {
			/**
		if (attrs.contains("bigint"))
			attrs = " type=\"multi64\"";
		else
			 */
			attrs = " type=\"multi\"";

		}

		if (field.default_value != null && !field.default_value.isEmpty())
			attrs += " default=\"" + StringEscapeUtils.escapeCsv(field.default_value) + "\"";

		filew.write(attrs + "/>\n");

	}

	/**
	 * Write Sphinx attribute in configuration file
	 * 
	 * @param table PosgtreSQL table
	 * @param field PostgreSQL field
	 * @param filew Sphinx configuration file writer
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeSphConfAttr(PgTable table, PgField field, FileWriter filew) throws IOException {

		String attr_name = table.name + "__" + field.xname;

		switch (field.xs_type) {
		case xs_boolean:
			filew.write("\txmlpipe_attr_bool       = " + attr_name + "\n");
			break;
		case xs_bigserial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_duration:
			if (field.sph_mva)
				filew.write("\txmlpipe_attr_multi_64   = " + attr_name + "\n");
			else
				filew.write("\txmlpipe_attr_bigint     = " + attr_name + "\n");
			break;
		case xs_serial:
		case xs_integer:
		case xs_int:
		case xs_nonPositiveInteger:
		case xs_negativeInteger:
		case xs_nonNegativeInteger:
		case xs_positiveInteger:
		case xs_unsignedInt:
		case xs_short:
		case xs_unsignedShort:
		case xs_byte:
		case xs_unsignedByte:
			if (field.sph_mva)
				filew.write("\txmlpipe_attr_multi      = " + attr_name + "\n");
			else
				filew.write("\txmlpipe_attr_uint       = " + attr_name + "\n");
			break;
		case xs_float:
		case xs_double:
		case xs_decimal:
			filew.write("\txmlpipe_attr_float      = " + attr_name + "\n");
			break;
		case xs_dateTime:
		case xs_time:
		case xs_date:
		case xs_gYearMonth:
		case xs_gYear:
			filew.write("\txmlpipe_attr_timestamp  = " + attr_name + "\n");
			break;
		case xs_hexBinary:
		case xs_base64Binary:
		case xs_gMonthDay:
		case xs_gMonth:
		case xs_gDay:
		case xs_anyType:
		case xs_string:
		case xs_normalizedString:
		case xs_token:
		case xs_language:
		case xs_Name:
		case xs_QName:
		case xs_NCName:
		case xs_anyURI:
		case xs_NOTATION:
		case xs_NMTOKEN:
		case xs_NMTOKENS:
		case xs_ID:
		case xs_IDREF:
		case xs_IDREFS:
		case xs_ENTITY:
		case xs_ENTITIES:
		case xs_any:
		case xs_anyAttribute:
			filew.write("\txmlpipe_attr_string     = " + attr_name + "\n");
			break;
		}

	}

	/** The date pattern in ISO 8601 format. */
	private static final String[] date_patterns_iso = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mmXXX", "yyyy-MM-dd'T'HHXXX", "yyyy-MM-ddXXX",
			"yyyy-MM'T'HH:mm:ss.SSSXXX", "yyyy-MM'T'HH:mm:ssXXX", "yyyy-MM'T'HH:mmXXX", "yyyy-MM'T'HHXXX", "yyyy-MMXXX",
			"yyyy'T'HH:mmXXX", "yyyy'T'HHXXX", "yyyyXXX",
			"yyyy-MM'T'HH:mmXXX", "yyyy-MM'T'HHXXX", "yyyy-MMXXX",
			"--MM-dd'T'HH:mmXXX", "--MM-dd'T'HHXXX", "--MM-ddXXX",
			"--dd'T'HH:mmXXX", "--dd'T'HHXXX", "--ddXXX"
	};

	/** The date pattern in UTC. */
	private static final String[] date_patterns_z = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm'Z'", "yyyy-MM-dd'T'HH'Z'", "yyyy-MM-dd'Z'",
			"yyyy-MM'T'HH:mm:ss.SSS'Z'", "yyyy-MM'T'HH:mm:ss'Z'", "yyyy-MM'T'HH:mm'Z'", "yyyy-MM'T'HH'Z'", "yyyy-MM'Z'",
			"yyyy'T'HH:mm'Z'", "yyyy'T'HH'Z'", "yyyy'Z'",
			"yyyy-MM'T'HH:mm'Z'", "yyyy-MM'T'HH'Z'", "yyyy-MM'Z'",
			"--MM-dd'T'HH:mm'Z'", "--MM-dd'T'HH'Z'", "--MM-dd'Z'",
			"--dd'T'HH:mm'Z'", "--dd'T'HH'Z'", "--dd'Z'"
	};

	/** The date pattern in local time. */
	private static final String[] date_patterns_loc = new String[] {
			"yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd'T'HH", "yyyy-MM-dd",
			"yyyy-MM'T'HH:mm:ss.SSS", "yyyy-MM'T'HH:mm:ss", "yyyy-MM'T'HH:mm", "yyyy-MM'T'HH", "yyyy-MM",
			"yyyy'T'HH:mm", "yyyy'T'HH", "yyyy",
			"yyyy-MM'T'HH:mm", "yyyy-MM'T'HH", "yyyy-MM",
			"--MM-dd'T'HH:mm", "--MM-dd'T'HH", "--MM-dd",
			"--dd'T'HH:mm", "--dd'T'HH", "--dd"
	};

	/**
	 * Parse string as java.util.Date.
	 *
	 * @param value content
	 * @return Date java.util.Date
	 */
	private static java.util.Date parseDate(String value) {

		java.util.Date date = null;

		try {

			date = DateUtils.parseDate(value, date_patterns_iso);

		} catch (ParseException e1) {

			try {

				date = DateUtils.parseDate(value, date_patterns_z);
				TimeZone tz = TimeZone.getDefault();
				int offset_sec = tz.getRawOffset() / 1000;
				date = DateUtils.addSeconds(date, offset_sec);

			} catch (ParseException e2) {

				try {

					date = DateUtils.parseDate(value, date_patterns_loc);

				} catch (ParseException e3) {
					return null;
				}

			}

		}

		return date;
	}

}
