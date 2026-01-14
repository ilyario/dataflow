# PutV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | [**ContentKeyV2**](ContentKeyV2.md) |  | 
**Content** | [**ContentV2**](ContentV2.md) |  | 
**Metadata** | Pointer to [**[]ContentMetadataV2**](ContentMetadataV2.md) |  | [optional] 
**Documentation** | Pointer to [**DocumentationV2**](DocumentationV2.md) |  | [optional] 

## Methods

### NewPutV2

`func NewPutV2(key ContentKeyV2, content ContentV2, ) *PutV2`

NewPutV2 instantiates a new PutV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPutV2WithDefaults

`func NewPutV2WithDefaults() *PutV2`

NewPutV2WithDefaults instantiates a new PutV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *PutV2) GetKey() ContentKeyV2`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *PutV2) GetKeyOk() (*ContentKeyV2, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *PutV2) SetKey(v ContentKeyV2)`

SetKey sets Key field to given value.


### GetContent

`func (o *PutV2) GetContent() ContentV2`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *PutV2) GetContentOk() (*ContentV2, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *PutV2) SetContent(v ContentV2)`

SetContent sets Content field to given value.


### GetMetadata

`func (o *PutV2) GetMetadata() []ContentMetadataV2`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *PutV2) GetMetadataOk() (*[]ContentMetadataV2, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *PutV2) SetMetadata(v []ContentMetadataV2)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *PutV2) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetDocumentation

`func (o *PutV2) GetDocumentation() DocumentationV2`

GetDocumentation returns the Documentation field if non-nil, zero value otherwise.

### GetDocumentationOk

`func (o *PutV2) GetDocumentationOk() (*DocumentationV2, bool)`

GetDocumentationOk returns a tuple with the Documentation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocumentation

`func (o *PutV2) SetDocumentation(v DocumentationV2)`

SetDocumentation sets Documentation field to given value.

### HasDocumentation

`func (o *PutV2) HasDocumentation() bool`

HasDocumentation returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


