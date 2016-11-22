## Annotations

Annotations provide information on how a particular field or model class
should behave. The most commonly used annotations are `@Indexed` and
`@Embedded`.

### Field Annotations

`@InternalNamePrefix(String)`

> Specifies the prefix for the internal names of all fields in the target type.

`@CollectionMaximum(int)`

> Specifies the maximum number of items in the target field.

`@CollectionMinimum(int)`

> Specifies the minimum number of items in the target field.

`@DisplayName(String)`

> Specifies the target field's display name.

`@Embedded`

> Specifies whether the target field value is embedded.

`@Ignored`

> Specifies whether the target field is ignored.

`@Indexed`

> Specifies whether the target field value is indexed.

`@InternalName(String)`

> Specifies the target field's internal name.

`@Maximum(double)`

> Specifies either the maximum numeric value or string length of the target field. Our example uses a 5 Star review option.

`@Minimum(double)`

> Specifies either the minimum numeric value or string length of the target field. The user can input 0 out of 5 for the review.

`@Step(double)`

> Specifies the margin between entries in the target field, in the example below every 0.5 is allowed. 0.5, 1.0, 1.5 etc.

`@Regex(String)`

> Specifies the regular expression pattern that the target field value must match.

`@Required`

> Specifies whether the target field value is required.
	
`@FieldTypes(Class<Recordable>[])`

> Specifies the valid types for the target field value.

`@FieldUnique`

> Deprecated. Use `Recordable.FieldIndexed` with isUnique instead.

`@Values`

> Specifies the valid values for the target field value.

### Class Annotations

`@Abstract`

> Specifies whether the target type is abstract and can't be used to create a concrete instance.

`@DisplayName(String)`

> Specifies the target type's display name.

`@Embedded`

> Specifies whether the target type data is always embedded within another type data.

`@InternalName(String)`

> Specifies the target type's internal name.

`@LabelFields(String[])`

> Specifies the field names that are used to retrieve the labels of the objects represented by the target type.

`@PreviewField`

> Specifies the field name used to retrieve the previews of the objects represented by the target type.

