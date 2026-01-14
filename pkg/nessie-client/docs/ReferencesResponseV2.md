# ReferencesResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**References** | [**[]ReferenceV2**](ReferenceV2.md) |  | 

## Methods

### NewReferencesResponseV2

`func NewReferencesResponseV2(references []ReferenceV2, ) *ReferencesResponseV2`

NewReferencesResponseV2 instantiates a new ReferencesResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferencesResponseV2WithDefaults

`func NewReferencesResponseV2WithDefaults() *ReferencesResponseV2`

NewReferencesResponseV2WithDefaults instantiates a new ReferencesResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *ReferencesResponseV2) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ReferencesResponseV2) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ReferencesResponseV2) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *ReferencesResponseV2) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *ReferencesResponseV2) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *ReferencesResponseV2) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *ReferencesResponseV2) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *ReferencesResponseV2) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetReferences

`func (o *ReferencesResponseV2) GetReferences() []ReferenceV2`

GetReferences returns the References field if non-nil, zero value otherwise.

### GetReferencesOk

`func (o *ReferencesResponseV2) GetReferencesOk() (*[]ReferenceV2, bool)`

GetReferencesOk returns a tuple with the References field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReferences

`func (o *ReferencesResponseV2) SetReferences(v []ReferenceV2)`

SetReferences sets References field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


