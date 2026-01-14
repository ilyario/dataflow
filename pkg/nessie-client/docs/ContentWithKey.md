# ContentWithKey

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | [**ContentKey**](ContentKey.md) |  | 
**Content** | [**Content**](Content.md) |  | 
**Documentation** | Pointer to [**Documentation**](Documentation.md) |  | [optional] 

## Methods

### NewContentWithKey

`func NewContentWithKey(key ContentKey, content Content, ) *ContentWithKey`

NewContentWithKey instantiates a new ContentWithKey object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentWithKeyWithDefaults

`func NewContentWithKeyWithDefaults() *ContentWithKey`

NewContentWithKeyWithDefaults instantiates a new ContentWithKey object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *ContentWithKey) GetKey() ContentKey`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *ContentWithKey) GetKeyOk() (*ContentKey, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *ContentWithKey) SetKey(v ContentKey)`

SetKey sets Key field to given value.


### GetContent

`func (o *ContentWithKey) GetContent() Content`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *ContentWithKey) GetContentOk() (*Content, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *ContentWithKey) SetContent(v Content)`

SetContent sets Content field to given value.


### GetDocumentation

`func (o *ContentWithKey) GetDocumentation() Documentation`

GetDocumentation returns the Documentation field if non-nil, zero value otherwise.

### GetDocumentationOk

`func (o *ContentWithKey) GetDocumentationOk() (*Documentation, bool)`

GetDocumentationOk returns a tuple with the Documentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocumentation

`func (o *ContentWithKey) SetDocumentation(v Documentation)`

SetDocumentation sets Documentation field to given value.

### HasDocumentation

`func (o *ContentWithKey) HasDocumentation() bool`

HasDocumentation returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


