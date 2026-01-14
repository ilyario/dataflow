# ReferencesResponseV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**References** | [**[]ReferenceV1**](ReferenceV1.md) |  | 

## Methods

### NewReferencesResponseV1

`func NewReferencesResponseV1(references []ReferenceV1, ) *ReferencesResponseV1`

NewReferencesResponseV1 instantiates a new ReferencesResponseV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferencesResponseV1WithDefaults

`func NewReferencesResponseV1WithDefaults() *ReferencesResponseV1`

NewReferencesResponseV1WithDefaults instantiates a new ReferencesResponseV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *ReferencesResponseV1) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ReferencesResponseV1) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ReferencesResponseV1) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *ReferencesResponseV1) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *ReferencesResponseV1) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *ReferencesResponseV1) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *ReferencesResponseV1) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *ReferencesResponseV1) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetReferences

`func (o *ReferencesResponseV1) GetReferences() []ReferenceV1`

GetReferences returns the References field if non-nil, zero value otherwise.

### GetReferencesOk

`func (o *ReferencesResponseV1) GetReferencesOk() (*[]ReferenceV1, bool)`

GetReferencesOk returns a tuple with the References field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReferences

`func (o *ReferencesResponseV1) SetReferences(v []ReferenceV1)`

SetReferences sets References field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


