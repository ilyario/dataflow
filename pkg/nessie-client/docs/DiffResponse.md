# DiffResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**Diffs** | Pointer to [**[]DiffEntry**](DiffEntry.md) |  | [optional] 
**EffectiveFromReference** | Pointer to [**Reference**](Reference.md) |  | [optional] 
**EffectiveToReference** | Pointer to [**Reference**](Reference.md) |  | [optional] 

## Methods

### NewDiffResponse

`func NewDiffResponse() *DiffResponse`

NewDiffResponse instantiates a new DiffResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDiffResponseWithDefaults

`func NewDiffResponseWithDefaults() *DiffResponse`

NewDiffResponseWithDefaults instantiates a new DiffResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *DiffResponse) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *DiffResponse) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *DiffResponse) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *DiffResponse) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *DiffResponse) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *DiffResponse) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *DiffResponse) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *DiffResponse) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetDiffs

`func (o *DiffResponse) GetDiffs() []DiffEntry`

GetDiffs returns the Diffs field if non-nil, zero value otherwise.

### GetDiffsOk

`func (o *DiffResponse) GetDiffsOk() (*[]DiffEntry, bool)`

GetDiffsOk returns a tuple with the Diffs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDiffs

`func (o *DiffResponse) SetDiffs(v []DiffEntry)`

SetDiffs sets Diffs field to given value.

### HasDiffs

`func (o *DiffResponse) HasDiffs() bool`

HasDiffs returns a boolean if a field has been set.

### GetEffectiveFromReference

`func (o *DiffResponse) GetEffectiveFromReference() Reference`

GetEffectiveFromReference returns the EffectiveFromReference field if non-nil, zero value otherwise.

### GetEffectiveFromReferenceOk

`func (o *DiffResponse) GetEffectiveFromReferenceOk() (*Reference, bool)`

GetEffectiveFromReferenceOk returns a tuple with the EffectiveFromReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveFromReference

`func (o *DiffResponse) SetEffectiveFromReference(v Reference)`

SetEffectiveFromReference sets EffectiveFromReference field to given value.

### HasEffectiveFromReference

`func (o *DiffResponse) HasEffectiveFromReference() bool`

HasEffectiveFromReference returns a boolean if a field has been set.

### GetEffectiveToReference

`func (o *DiffResponse) GetEffectiveToReference() Reference`

GetEffectiveToReference returns the EffectiveToReference field if non-nil, zero value otherwise.

### GetEffectiveToReferenceOk

`func (o *DiffResponse) GetEffectiveToReferenceOk() (*Reference, bool)`

GetEffectiveToReferenceOk returns a tuple with the EffectiveToReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveToReference

`func (o *DiffResponse) SetEffectiveToReference(v Reference)`

SetEffectiveToReference sets EffectiveToReference field to given value.

### HasEffectiveToReference

`func (o *DiffResponse) HasEffectiveToReference() bool`

HasEffectiveToReference returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


