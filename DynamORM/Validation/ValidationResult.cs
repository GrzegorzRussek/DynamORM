using DynamORM.Mapper;

namespace DynamORM.Validation
{
    /// <summary>Validation result.</summary>
    public class ValidationResult
    {
        /// <summary>Gets the property invoker.</summary>
        public DynamicPropertyInvoker Property { get; internal set; }

        /// <summary>Gets the requirement definition.</summary>
        public RequiredAttribute Requirement { get; internal set; }

        /// <summary>Gets the value that is broken.</summary>
        public object Value { get; internal set; }

        /// <summary>Gets the result.</summary>
        public ValidateResult Result { get;internal set;}
    }
}