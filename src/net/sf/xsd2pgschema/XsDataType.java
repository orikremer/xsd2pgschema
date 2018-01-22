/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2014-2018 Masashi Yokochi

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

import org.apache.commons.text.StringEscapeUtils;

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
	 * @param xs_type compared data type
	 * @return least common data type
	 */
	public XsDataType leastCommonOf(XsDataType xs_type) {

		if (xs_type == null)
			return xs_anyType;

		if (this.equals(xs_type))
			return this;

		switch (this) {
		case xs_boolean:
			return xs_anyType;
		case xs_hexBinary:
		case xs_base64Binary:
			switch (xs_type) {
			case xs_hexBinary:
			case xs_base64Binary:
				return this;
			default:
				return xs_anyType;
			}
		case xs_bigserial:
			switch (xs_type) {
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
				return xs_type;
			case xs_serial:
				return xs_bigserial;
			default:
				return xs_anyType;
			}
		case xs_long:
		case xs_bigint:
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_unsignedLong:
			switch (xs_type) {
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_serial:
		case xs_positiveInteger:
			switch (xs_type) {
			case xs_bigserial:
				return xs_type;
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_integer:
		case xs_int:
			switch (xs_type) {
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_nonPositiveInteger:
			switch (xs_type) {
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_negativeInteger:
			switch (xs_type) {
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
				return xs_type;
			default:
				return xs_anyType;
			}
		case xs_nonNegativeInteger:
		case xs_unsignedInt:
			switch (xs_type) {
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
				return this;
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
			switch (xs_type) {
			case xs_double:
			case xs_decimal:
				return xs_type;
			default:
				return xs_anyType;
			}
		case xs_double:
			switch (xs_type) {
			case xs_float:
				return this;
			case xs_decimal:
				return xs_type;
			default:
				return xs_anyType;
			}
		case xs_decimal:
			switch (xs_type) {
			case xs_float:
			case xs_double:
				return this;
			default:
				return xs_anyType;
			}
		case xs_short:
		case xs_byte:
			switch (xs_type) {
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
				return this;
			default:
				return xs_anyType;
			}
		case xs_unsignedShort:
		case xs_unsignedByte:
			switch (xs_type) {
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
				return xs_type;
			case xs_unsignedShort:
			case xs_unsignedByte:
				return this;
			default:
				return xs_anyType;
			}
		case xs_dateTime:
			switch (xs_type) {
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				return this;
			default:
				return xs_anyType;
			}
		case xs_date:
			switch (xs_type) {
			case xs_dateTime:
				return xs_type;
			case xs_gYearMonth:
			case xs_gYear:
				return this;
			default:
				return xs_anyType;
			}
		case xs_gYearMonth:
			switch (xs_type) {
			case xs_dateTime:
			case xs_date:
				return xs_type;
			case xs_gYear:
				return this;
			default:
				return xs_anyType;
			}
		case xs_gYear:
			switch (xs_type) {
			case xs_dateTime:
			case xs_date:
			case xs_gYearMonth:
				return xs_type;
			default:
				return xs_anyType;
			}
		case xs_gMonthDay:
			switch (xs_type) {
			case xs_gMonth:
				return this;
			default:
				return xs_anyType;
			}
		case xs_gMonth:
			switch (xs_type) {
			case xs_gMonthDay:
				return xs_type;
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
			switch (xs_type) {
			case xs_anyAttribute:
				return xs_any;
			default:
				return xs_anyType;
			}
		case xs_anyAttribute:
			switch (xs_type) {
			case xs_any:
				return xs_any;
			default:
				return xs_anyType;
			}
		}

		return xs_anyType;
	}

	// JSON Schema generation

	/**
	 * Return JSON Schema type definition.
	 *
	 * @return String JSON Schema type definition
	 */
	public String getJsonSchemaType() {

		switch (this) {
		case xs_boolean:
			return "\"boolean\"";
		case xs_bigserial:
		case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_long:
		case xs_bigint:
		case xs_integer:
		case xs_int:
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
	 * Return JSON Schema $ref.
	 *
	 * @return String JSON Schema $ref definition
	 */
	public String getJsonSchemaRef() {
		return "http://www.jsonix.org/jsonschemas/w3c/2001/XMLSchema.jsonschema#/definitions/" + this.toString().replaceFirst("^xs_", "");
	}

	// Type dependent attribute selection for full-text indexing

	/**
	 * Append field as index attribute.
	 *
	 * @param table PosgtreSQL table
	 * @param field PostgreSQL field
	 * @param index_filter index filter
	 */
	public static void appendAttr(PgTable table, PgField field, IndexFilter index_filter) {

		switch (field.xs_type) {
		case xs_bigserial:
		case xs_serial:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
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
		default: // free text
			if (index_filter.attr_string)
				index_filter.addAttr(table.name + "." + field.name);
		}

	}

	// Sphinx schema generation

	/**
	 * Write Sphinx attribute in schema file
	 *
	 * @param table PosgtreSQL table
	 * @param field PostgreSQL field
	 * @param filew Sphinx schema file writer
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeSphSchemaAttr(PgTable table, PgField field, FileWriter filew) throws IOException {

		filew.write("<sphinx:attr name=\"" + table.name + PgSchemaUtil.sph_member_op + field.xname + "\"");

		String attrs = null;

		switch (field.xs_type) {
		case xs_boolean:
			attrs = " type=\"bool\"";
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
		default: // string
			attrs = " type=\"string\"";
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
			attrs += " default=\"" + StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeXml10(field.default_value)) + "\"";

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

		String attr_name = table.name + PgSchemaUtil.sph_member_op + field.xname;

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
		default: // string
			filew.write("\txmlpipe_attr_string     = " + attr_name + "\n");
		}

	}

}
