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

package net.sf.xsd2pgschema.type;

/**
 * Enumerator of field type.
 *
 * @author yokochi
 */
public enum XsFieldType {

	// boolean - primitive types

	/** The xs:boolean. */
	xs_boolean,

	// binary - primitive types

	/** The xs:hexBinary. */
	xs_hexBinary,
	/** The xs:base64Binary. */
	xs_base64Binary,

	// serial number - not built-in data types
	/*
	 ** The xs:bigserial. *
	xs_bigserial,
	 ** The xs:serial. *
	xs_serial,
	 */

	// decimal - primitive types

	/** The xs:decimal. */
	xs_decimal,
	/** The xs:double. */
	xs_double,
	/** The xs:float. */
	xs_float,

	// finite integer - atomic types

	/** The xs:long. */
	xs_long,
	/** The xs:bigint. */
	xs_bigint,
	/** The xs:int. */
	xs_int,
	/** The xs:short. */
	xs_short,
	/** The xs:byte. */
	xs_byte,

	/** The xs:unsignedLong. */
	xs_unsignedLong,
	/** The xs:unsignedInt. */
	xs_unsignedInt,
	/** The xs:unsignedShort. */
	xs_unsignedShort,
	/** The xs:unsignedByte. */
	xs_unsignedByte,

	// infinite integer - atomic types

	/** The xs:integer. */
	xs_integer,
	/** The xs:nonNegativeInteger. */
	xs_nonNegativeInteger,
	/** The xs:nonPositiveInteger. */
	xs_nonPositiveInteger,
	/** The xs:positiveInteger. */
	xs_positiveInteger,
	/** The xs:negativeInteger. */
	xs_negativeInteger,

	// duration - primitive types

	/** The xs:duration. */
	xs_duration,
	/** The xs:yearMonthDuration. */
	xs_yearMonthDuration,
	/** The xs:dayTimeduration. */
	xs_dayTimeDuration,

	// time - primitive types

	/** The xs:dateTime. */
	xs_dateTime,
	/** The xs:dateTimeStamp. */
	xs_dateTimeStamp,

	/** The xs:date. */
	xs_date,
	/** The xs:time. */
	xs_time,

	/** The xs:gYear. */
	xs_gYear,
	/** The xs:gYearMonth. */
	xs_gYearMonth,
	/** The xs:gMonth. */
	xs_gMonth,
	/** The xs:gMonthDay. */
	xs_gMonthDay,
	/** The xs:gDay. */
	xs_gDay,

	// string - primitive types

	/** The xs:string. */
	xs_string,
	/** The xs:anyURI. */
	xs_anyURI,
	/** The xs:QName. */
	xs_QName,
	/** The xs:NOTATION. */
	xs_NOTATION,

	// string - atomic types

	/** The xs:normalizedString. */
	xs_normalizedString,
	/** The xs:token. */
	xs_token,
	/** The xs:language. */
	xs_language,
	/** The xs:Name. */
	xs_Name,
	/** The xs:NCName. */
	xs_NCName,
	/** The xs:ENTITY. */
	xs_ENTITY,
	/** The xs:ID. */
	xs_ID,
	/** The xs:IDREF. */
	xs_IDREF,
	/** The xs:NMTOKEN. */
	xs_NMTOKEN,

	// build-in list types

	/** The xs:ENTITIES. */
	xs_ENTITIES,
	/** The xs:IDREFS. */
	xs_IDREFS,
	/** The xs:NMTOKENS. */
	xs_NMTOKENS,

	// wild card - special types

	/** The xs:anyType. */
	xs_anyType,
	/** The xs:any. */
	xs_any,
	/** The xs:anyAttribute. */
	xs_anyAttribute;

	/**
	 * Return least common type.
	 *
	 * @param xs_type compared type
	 * @param map_big_integer whether map xs:integer to BigInteger
	 * @return XsDataType the least common type
	 */
	public XsFieldType leastCommonOf(XsFieldType xs_type, boolean map_big_integer) {

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
			// case xs_bigserial:
			// case xs_serial:
		case xs_integer:
		case xs_nonNegativeInteger:
		case xs_nonPositiveInteger:
		case xs_positiveInteger:
		case xs_negativeInteger:
		case xs_long:
		case xs_bigint:
		case xs_unsignedLong:
		case xs_int:
		case xs_unsignedInt:
		case xs_short:
		case xs_unsignedShort:
		case xs_byte:
		case xs_unsignedByte:

			// strict W3C rule (map xs:integer to BigInteger)

			if (map_big_integer) {

				switch (this) {
				/*
				case xs_bigserial:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
					case xs_unsignedLong:
						return xs_type;
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						return xs_integer;
					case xs_long:
					case xs_bigint:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_long;
					case xs_unsignedInt:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedLong;
					case xs_serial:
						return this;
					default:
						return xs_anyType;
					}
				case xs_serial:
					switch (xs_type) {
					case xs_integer:
					case xs_bigserial:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
					case xs_unsignedLong:
					case xs_unsignedInt:
						return xs_type;
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						return xs_integer;
					case xs_long:
					case xs_bigint:
						return xs_long;
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_int;
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedInt;
					default:
						return xs_anyType;
					}
				 */
				case xs_long:
				case xs_bigint:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						return xs_integer;
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_long;
					default:
						return xs_anyType;
					}
				case xs_int:
					switch (xs_type) {
					case xs_integer:
						return xs_type;
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						return xs_integer;
						// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
						// case xs_serial:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return this;
					default:
						return xs_anyType;
					}
				case xs_short:
				case xs_byte:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						return xs_integer;
						// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
						// case xs_serial:
					case xs_int:
					case xs_unsignedInt:
						return xs_int;
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_short;
					default:
						return xs_anyType;
					}
				case xs_unsignedLong:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
						return xs_type;
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						return xs_integer;
					case xs_long:
					case xs_bigint:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_long;
						// case xs_bigserial:
						// case xs_serial:
					case xs_unsignedInt:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return this;
					case xs_positiveInteger:
						return xs_nonNegativeInteger;
					default:
						return xs_anyType;
					}
				case xs_unsignedInt:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_type;
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						return xs_integer;
						// case xs_bigserial:
						// 	return xs_unsignedLong;
						// case xs_serial:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return this;
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_int;
					case xs_positiveInteger:
						return xs_nonNegativeInteger;
					default:
						return xs_anyType;
					}
				case xs_unsignedShort:
				case xs_unsignedByte:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						return xs_integer;
						// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
						// case xs_serial:
					case xs_int:
					case xs_unsignedInt:
						return xs_int;
					case xs_short:
					case xs_byte:
						return xs_type;
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedShort;
					default:
						return xs_anyType;
					}
				case xs_integer:
					switch (xs_type) {
					// case xs_bigserial:
					// case xs_serial:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return this;
					default:
						return xs_anyType;
					}
				case xs_nonNegativeInteger:
					switch (xs_type) {
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_integer;
					case xs_positiveInteger:
						return this;
					default:
						return xs_anyType;
					}
				case xs_nonPositiveInteger:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_integer;
					case xs_negativeInteger:
						return this;
					default:
						return xs_anyType;
					}
				case xs_positiveInteger:
					switch (xs_type) {
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_integer;
					case xs_nonNegativeInteger:
						return xs_type;
					default:
						return xs_anyType;
					}
				case xs_negativeInteger:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_integer;
					case xs_nonPositiveInteger:
						return xs_type;
					default:
						return xs_anyType;
					}
				default:
					break;
				}

			}

			// relaxed W3C rule (map xs:integer to xs:int)

			else {

				switch (this) {
				/*
				case xs_bigserial:
					switch (xs_type) {
					case xs_unsignedLong:
						return xs_type;
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
					case xs_long:
					case xs_bigint:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_long;
					case xs_nonNegativeInteger:
					case xs_unsignedInt:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedLong;
					case xs_positiveInteger:
					case xs_serial:
						return this;
					default:
						return xs_anyType;
					}
				case xs_serial:
				case xs_positiveInteger:
					switch (xs_type) {
					case xs_bigserial:
					case xs_unsignedLong:
					case xs_unsignedInt:
						return xs_type;
					case xs_long:
					case xs_bigint:
						return xs_long;
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_int;
					case xs_nonNegativeInteger:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedInt;
					case xs_positiveInteger:
						return xs_serial;
					default:
						return xs_anyType;
					}
				 */
				case xs_long:
				case xs_bigint:
					switch (xs_type) {
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
					case xs_int:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_long;
					default:
						return xs_anyType;
					}
				case xs_int:
				case xs_integer:
					switch (xs_type) {
					// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						// case xs_serial:
					case xs_unsignedInt:
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_int;
					default:
						return xs_anyType;
					}
				case xs_short:
				case xs_byte:
					switch (xs_type) {
					// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:
						// case xs_serial:
					case xs_int:
					case xs_unsignedInt:
						return xs_int;
					case xs_short:
					case xs_byte:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_short;
					default:
						return xs_anyType;
					}
				case xs_unsignedLong:
					switch (xs_type) {
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
					case xs_long:
					case xs_bigint:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_long;
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_bigserial:
						// case xs_serial:
					case xs_unsignedInt:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return this;
					default:
						return xs_anyType;
					}
				case xs_unsignedInt:
				case xs_nonNegativeInteger:
					switch (xs_type) {
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_type;
						// case xs_bigserial:
						//	return xs_unsignedLong;
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_serial:
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedInt;
					case xs_integer:
					case xs_nonPositiveInteger:
					case xs_negativeInteger:
					case xs_int:
					case xs_short:
					case xs_byte:
						return xs_int;
					default:
						return xs_anyType;
					}
				case xs_unsignedShort:
				case xs_unsignedByte:
					switch (xs_type) {
					// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_nonPositiveInteger:
					case xs_positiveInteger:
					case xs_negativeInteger:	
						// case xs_serial:
					case xs_int:
					case xs_unsignedInt:
						return xs_int;
					case xs_short:
					case xs_byte:
						return xs_type;
					case xs_unsignedShort:
					case xs_unsignedByte:
						return xs_unsignedShort;
					default:
						return xs_anyType;
					}
				case xs_nonPositiveInteger:
					switch (xs_type) {
					// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_serial:
					case xs_int:
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
					// case xs_bigserial:
					case xs_long:
					case xs_bigint:
					case xs_unsignedLong:
						return xs_long;
					case xs_integer:
					case xs_nonNegativeInteger:
					case xs_positiveInteger:
						// case xs_serial:
					case xs_int:
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
				default:
					break;
				}

			}
			break;
		case xs_decimal:
			switch (xs_type) {
			case xs_float:
			case xs_double:
				return this;
			default:
				return xs_anyType;
			}
		case xs_double:
			switch (xs_type) {
			case xs_decimal:
				return xs_type;
			case xs_float:
				return this;
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
		case xs_dateTime:
			switch (xs_type) {
			case xs_dateTimeStamp:
			case xs_date:
			case xs_gYearMonth:
			case xs_gYear:
				return this;
			default:
				return xs_anyType;
			}
		case xs_dateTimeStamp:
			switch (xs_type) {
			case xs_dateTime:
				return xs_type;
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
			case xs_dateTimeStamp:
				return xs_type;
			case xs_gYearMonth:
			case xs_gYear:
				return this;
			default:
				return xs_anyType;
			}
		case xs_gYear:
			switch (xs_type) {
			case xs_dateTime:
			case xs_dateTimeStamp:
			case xs_date:
			case xs_gYearMonth:
				return xs_type;
			default:
				return xs_anyType;
			}
		case xs_gYearMonth:
			switch (xs_type) {
			case xs_dateTime:
			case xs_dateTimeStamp:
			case xs_date:
				return xs_type;
			case xs_gYear:
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
		case xs_gMonthDay:
			switch (xs_type) {
			case xs_gMonth:
				return this;
			default:
				return xs_anyType;
			}
		case xs_duration:
		case xs_yearMonthDuration:
		case xs_dayTimeDuration:
		case xs_time:
		case xs_gDay:
		case xs_string:
		case xs_anyURI:
		case xs_QName:
		case xs_NOTATION:
		case xs_normalizedString:
		case xs_token:
		case xs_language:
		case xs_Name:
		case xs_NCName:
		case xs_ENTITY:
		case xs_ID:
		case xs_IDREF:	
		case xs_NMTOKEN:
		case xs_ENTITIES:
		case xs_IDREFS:
		case xs_NMTOKENS:
		case xs_anyType:
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
			// case xs_bigserial:
			// case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_nonNegativeInteger:
		case xs_nonPositiveInteger:
		case xs_positiveInteger:
		case xs_negativeInteger:
		case xs_long:
		case xs_bigint:
		case xs_int:
		case xs_short:
		case xs_byte:
		case xs_unsignedLong:
		case xs_unsignedInt:
		case xs_unsignedShort:
		case xs_unsignedByte:
			return "\"number\"";
		case xs_hexBinary:
		case xs_base64Binary:
		case xs_duration:
		case xs_yearMonthDuration:
		case xs_dayTimeDuration:
		case xs_dateTime:
		case xs_dateTimeStamp:
		case xs_date:
		case xs_time:
		case xs_gYear:
		case xs_gYearMonth:
		case xs_gMonth:
		case xs_gMonthDay:
		case xs_gDay:
		case xs_string:
		case xs_anyURI:
		case xs_QName:
		case xs_NOTATION:
		case xs_normalizedString:
		case xs_token:
		case xs_language:
		case xs_Name:
		case xs_NCName:
		case xs_ENTITY:
		case xs_ID:
		case xs_IDREF:	
		case xs_NMTOKEN:
		case xs_ENTITIES:
		case xs_IDREFS:
		case xs_NMTOKENS:
			return "\"string\"";
		case xs_any:
		case xs_anyAttribute:
			return "\"object\"";
		default:
		}

		return "null";
	}

	/**
	 * Return JSON Schema $ref.
	 *
	 * @return String JSON Schema $ref definition
	 */
	public String getJsonSchemaRef() {

		switch (this) {
		case xs_any:
		case xs_anyAttribute:
			return null;
		default:
			return "http://www.jsonix.org/jsonschemas/w3c/2001/XMLSchema.jsonschema#/definitions/" + this.toString().substring(3);
		}

	}

	// XPath evaluation

	/**
	 * Return whether Latin-1 encoded.
	 * 
	 * @return boolean whether Latin-1 encoded
	 */
	public boolean isLatin1Encoded() {

		switch (this) {
		case xs_boolean:
		case xs_hexBinary:
		case xs_base64Binary:
			// case xs_bigserial:
			// case xs_serial:
		case xs_float:
		case xs_double:
		case xs_decimal:
		case xs_integer:
		case xs_nonNegativeInteger:
		case xs_nonPositiveInteger:
		case xs_positiveInteger:
		case xs_negativeInteger:
		case xs_long:
		case xs_bigint:
		case xs_int:
		case xs_short:
		case xs_byte:
		case xs_unsignedLong:
		case xs_unsignedInt:
		case xs_unsignedShort:
		case xs_unsignedByte:
		case xs_duration:
		case xs_yearMonthDuration:
		case xs_dayTimeDuration:
		case xs_dateTime:
		case xs_dateTimeStamp:
		case xs_date:
		case xs_time:
		case xs_gYear:
		case xs_gYearMonth:
		case xs_gMonth:
		case xs_gMonthDay:
		case xs_gDay:
			return true;
		default:
			return false;
		}

	}

}
