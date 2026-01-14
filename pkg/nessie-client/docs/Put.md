# Put

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | [**ContentKey**](ContentKey.md) |  | 
**Content** | [**Content**](Content.md) |  | 
**ExpectedContent** | Pointer to [**Content**](Content.md) |  | [optional] 
**Metadata** | Pointer to [**[]ContentMetadata**](ContentMetadata.md) |  | [optional] 
**Documentation** | Pointer to [**Documentation**](Documentation.md) |  | [optional] 

## Methods

### NewPut

`func NewPut(key ContentKey, content Content, ) *Put`

NewPut instantiates a new Put object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPutWithDefaults

`func NewPutWithDefaults() *Put`

NewPutWithDefaults instantiates a new Put object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *Put) GetKey() ContentKey`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *Put) GetKeyOk() (*ContentKey, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *Put) SetKey(v ContentKey)`

SetKey sets Key field to given value.


### GetContent

`func (o *Put) GetContent() Content`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *Put) GetContentOk() (*Content, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *Put) SetContent(v Content)`

SetContent sets Content field to given value.


### GetExpectedContent

`func (o *Put) GetExpectedContent() Content`

GetExpectedContent returns the ExpectedContent field if non-nil, zero value otherwise.

### GetExpectedContentOk

`func (o *Put) GetExpectedContentOk() (*Content, bool)`

GetExpectedContentOk returns a tuple with the ExpectedContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedContent

`func (o *Put) SetExpectedContent(v Content)`

SetExpectedContent sets ExpectedContent field to given value.

### HasExpectedContent

`func (o *Put) HasExpectedContent() bool`

HasExpectedContent returns a boolean if a field has been set.

### GetMetadata

`func (o *Put) GetMetadata() []ContentMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Put) GetMetadataOk() (*[]ContentMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Put) SetMetadata(v []ContentMetadata)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Put) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetDocumentation

`func (o *Put) GetDocumentation() Documentation`

GetDocumentation returns the Documentation field if non-nil, zero value otherwise.

### GetDocumentationOk

`func (o *Put) GetDocumentationOk() (*Documentation, bool)`

GetDocumentationOk returns a tuple with the Documentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocumentation

`func (o *Put) SetDocumentation(v Documentation)`

SetDocumentation sets Documentation field to given value.

### HasDocumentation

`func (o *Put) HasDocumentation() bool`

HasDocumentation returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


