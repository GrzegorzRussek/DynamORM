using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DynamORM.Mapper;

namespace DynamORM.Validation
{
    /// <summary>Required attribute can be used to validate fields in objects using mapper class.</summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class RequiredAttribute : Attribute
    {
        /// <summary>Gets or sets minimum value or length of field.</summary>
        public decimal? Min { get; set; }

        /// <summary>Gets or sets maximum value or length of field.</summary>
        public decimal? Max { get; set; }

        /// <summary>Gets or sets pattern to verify.</summary>
        public Regex Pattern { get; set; }

        /// <summary>Gets or sets a value indicating whether property value is required or not.</summary>
        public bool Required { get; set; }

        /// <summary>Gets or sets a value indicating whether this is an element requirement.</summary>
        public bool ElementRequirement { get; set; }

        /// <summary>Initializes a new instance of the <see cref="RequiredAttribute" /> class.</summary>
        /// <param name="required">This field will be required.</param>
        public RequiredAttribute(bool required = true)
        {
            Required = required;
        }

        /// <summary>Initializes a new instance of the <see cref="RequiredAttribute" /> class.</summary>
        /// <param name="val">Limiting value to set.</param>
        /// <param name="max">Whether set maximum parameter (true) or minimum parameter (false).</param>
        /// <param name="required">This field will be required.</param>
        public RequiredAttribute(float val, bool max, bool required = true)
        {
            if (max)
                Max = (decimal)val;
            else
                Min = (decimal)val;
            Required = required;
        }

        /// <summary>Initializes a new instance of the <see cref="RequiredAttribute" /> class.</summary>
        /// <param name="min">Minimum value to set.</param>
        /// <param name="max">Maximum value to set.</param>
        /// <param name="required">This field will be required.</param>
        public RequiredAttribute(float min, float max, bool required = true)
        {
            Min = (decimal)min;
            Max = (decimal)max;
            Required = required;
        }

        /// <summary>Initializes a new instance of the <see cref="RequiredAttribute" /> class.</summary>
        /// <param name="min">Minimum value to set.</param>
        /// <param name="max">Maximum value to set.</param>
        /// <param name="pattern">Pattern to check.</param>
        /// <param name="required">This field will be required.</param>
        public RequiredAttribute(float min, float max, string pattern, bool required = true)
        {
            Min = (decimal)min;
            Max = (decimal)max;
            Pattern = new Regex(pattern, RegexOptions.Compiled);
            Required = required;
        }

        internal ValidateResult ValidateSimpleValue(DynamicPropertyInvoker dpi, object val)
        {
            return ValidateSimpleValue(dpi.Type, dpi.IsGnericEnumerable, val);
        }

        internal ValidateResult ValidateSimpleValue(Type type, bool isGnericEnumerable, object val)
        {
            if (val == null)
            {
                if (Required)
                    return ValidateResult.ValueIsMissing;
                else
                    return ValidateResult.Valid;
            }

            if (type.IsValueType)
            {
                if (val is decimal || val is long || val is int || val is float || val is double || val is short || val is byte ||
                    val is decimal? || val is long? || val is int? || val is float? || val is double? || val is short? || val is byte?)
                {
                    decimal dec = Convert.ToDecimal(val);

                    if (Min.HasValue && Min.Value > dec)
                        return ValidateResult.ValueTooSmall;

                    if (Max.HasValue && Max.Value < dec)
                        return ValidateResult.ValueTooLarge;

                    return ValidateResult.Valid;
                }
                else
                {
                    var str = val.ToString();

                    if (Min.HasValue && Min.Value > str.Length)
                        return ValidateResult.ValueTooShort;

                    if (Max.HasValue && Max.Value < str.Length)
                        return ValidateResult.ValueTooLong;

                    if (Pattern != null && !Pattern.IsMatch(str))
                        return ValidateResult.ValueDontMatchPattern;

                    return ValidateResult.Valid;
                }
            }
            else if (type.IsArray || isGnericEnumerable)
            {
                int? cnt = null;

                var list = (val as IEnumerable<object>);
                if (list != null)
                    cnt = list.Count();
                else
                {
                    var enumerable = (val as IEnumerable);
                    if (enumerable != null)
                        cnt = enumerable.Cast<object>().Count();
                }

                if (Min.HasValue && Min.Value > cnt)
                    return ValidateResult.TooFewElementsInCollection;

                if (Max.HasValue && Max.Value < cnt)
                    return ValidateResult.TooManyElementsInCollection;

                return ValidateResult.Valid;
            }
            else if (type == typeof(string))
            {
                var str = (string)val;

                if (Min.HasValue && Min.Value > str.Length)
                    return ValidateResult.ValueTooShort;

                if (Max.HasValue && Max.Value < str.Length)
                    return ValidateResult.ValueTooLong;

                if (Pattern != null && !Pattern.IsMatch(str))
                    return ValidateResult.ValueDontMatchPattern;

                return ValidateResult.Valid;
            }

            return ValidateResult.NotSupported;
        }
    }
}