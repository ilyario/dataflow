# TagV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadataV2**](ReferenceMetadataV2.md) |  | [optional] 
**Hash** | Pointer to **string** |  | [optional] 

## Methods

### NewTagV2

`func NewTagV2(name string, ) *TagV2`

NewTagV2 instantiates a new TagV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTagV2WithDefaults

`func NewTagV2WithDefaults() *TagV2`

NewTagV2WithDefaults instantiates a new TagV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *TagV2) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *TagV2) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *TagV2) SetName(v string)`

SetName sets Name field to given value.


### GetMetadata

`func (o *TagV2) GetMetadata() ReferenceMetadataV2`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *TagV2) GetMetadataOk() (*ReferenceMetadataV2, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *TagV2) SetMetadata(v ReferenceMetadataV2)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *TagV2) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetHash

`func (o *TagV2) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *TagV2) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *TagV2) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *TagV2) HasHash() bool`

HasHash returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


