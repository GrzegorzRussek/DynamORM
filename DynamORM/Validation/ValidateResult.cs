namespace DynamORM.Validation
{
    /// <summary>Validation result enum.</summary>
    public enum ValidateResult
    {
        /// <summary>The valid value.</summary>
        Valid,

        /// <summary>The value is missing.</summary>
        ValueIsMissing,

        /// <summary>The value too small.</summary>
        ValueTooSmall,

        /// <summary>The value too large.</summary>
        ValueTooLarge,

        /// <summary>The too few elements in collection.</summary>
        TooFewElementsInCollection,

        /// <summary>The too many elements in collection.</summary>
        TooManyElementsInCollection,

        /// <summary>The value too short.</summary>
        ValueTooShort,

        /// <summary>The value too long.</summary>
        ValueTooLong,

        /// <summary>The value don't match pattern.</summary>
        ValueDontMatchPattern,

        /// <summary>The not supported.</summary>
        NotSupported,
    }
}