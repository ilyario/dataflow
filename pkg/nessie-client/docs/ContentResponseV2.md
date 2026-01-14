# ContentResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Content** | [**ContentV2**](ContentV2.md) |  | 
**EffectiveReference** | [**ReferenceV2**](ReferenceV2.md) |  | 
**Documentation** | Pointer to [**DocumentationV2**](DocumentationV2.md) |  | [optional] 

## Methods

### NewContentResponseV2

`func NewContentResponseV2(content ContentV2, effectiveReference ReferenceV2, ) *ContentResponseV2`

NewContentResponseV2 instantiates a new ContentResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentResponseV2WithDefaults

`func NewContentResponseV2WithDefaults() *ContentResponseV2`

NewContentResponseV2WithDefaults instantiates a new ContentResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetContent

`func (o *ContentResponseV2) GetContent() ContentV2`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *ContentResponseV2) GetContentOk() (*ContentV2, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *ContentResponseV2) SetContent(v ContentV2)`

SetContent sets Content field to given value.


### GetEffectiveReference

`func (o *ContentResponseV2) GetEffectiveReference() ReferenceV2`

GetEffectiveReference returns the EffectiveReference field if non-nil, zero value otherwise.

### GetEffectiveReferenceOk

`func (o *ContentResponseV2) GetEffectiveReferenceOk() (*ReferenceV2, bool)`

GetEffectiveReferenceOk returns a tuple with the EffectiveReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveReference

`func (o *ContentResponseV2) SetEffectiveReference(v ReferenceV2)`

SetEffectiveReference sets EffectiveReference field to given value.


### GetDocumentation

`func (o *ContentResponseV2) GetDocumentation() DocumentationV2`

GetDocumentation returns the Documentation field if non-nil, zero value otherwise.

### GetDocumentationOk

`func (o *ContentResponseV2) GetDocumentationOk() (*DocumentationV2, bool)`

GetDocumentationOk returns a tuple with the Documentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocumentation

`func (o *ContentResponseV2) SetDocumentation(v DocumentationV2)`

SetDocumentation sets Documentation field to given value.

### HasDocumentation

`func (o *ContentResponseV2) HasDocumentation() bool`

HasDocumentation returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


