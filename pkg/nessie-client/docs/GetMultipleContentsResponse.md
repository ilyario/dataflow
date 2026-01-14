# GetMultipleContentsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Contents** | [**[]ContentWithKey**](ContentWithKey.md) |  | 
**EffectiveReference** | Pointer to [**Reference**](Reference.md) |  | [optional] 

## Methods

### NewGetMultipleContentsResponse

`func NewGetMultipleContentsResponse(contents []ContentWithKey, ) *GetMultipleContentsResponse`

NewGetMultipleContentsResponse instantiates a new GetMultipleContentsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetMultipleContentsResponseWithDefaults

`func NewGetMultipleContentsResponseWithDefaults() *GetMultipleContentsResponse`

NewGetMultipleContentsResponseWithDefaults instantiates a new GetMultipleContentsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetContents

`func (o *GetMultipleContentsResponse) GetContents() []ContentWithKey`

GetContents returns the Contents field if non-nil, zero value otherwise.

### GetContentsOk

`func (o *GetMultipleContentsResponse) GetContentsOk() (*[]ContentWithKey, bool)`

GetContentsOk returns a tuple with the Contents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContents

`func (o *GetMultipleContentsResponse) SetContents(v []ContentWithKey)`

SetContents sets Contents field to given value.


### GetEffectiveReference

`func (o *GetMultipleContentsResponse) GetEffectiveReference() Reference`

GetEffectiveReference returns the EffectiveReference field if non-nil, zero value otherwise.

### GetEffectiveReferenceOk

`func (o *GetMultipleContentsResponse) GetEffectiveReferenceOk() (*Reference, bool)`

GetEffectiveReferenceOk returns a tuple with the EffectiveReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveReference

`func (o *GetMultipleContentsResponse) SetEffectiveReference(v Reference)`

SetEffectiveReference sets EffectiveReference field to given value.

### HasEffectiveReference

`func (o *GetMultipleContentsResponse) HasEffectiveReference() bool`

HasEffectiveReference returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


