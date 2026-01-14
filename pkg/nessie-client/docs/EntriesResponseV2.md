# EntriesResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**Entries** | [**[]EntryV2**](EntryV2.md) |  | 
**EffectiveReference** | Pointer to [**ReferenceV2**](ReferenceV2.md) |  | [optional] 

## Methods

### NewEntriesResponseV2

`func NewEntriesResponseV2(entries []EntryV2, ) *EntriesResponseV2`

NewEntriesResponseV2 instantiates a new EntriesResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewEntriesResponseV2WithDefaults

`func NewEntriesResponseV2WithDefaults() *EntriesResponseV2`

NewEntriesResponseV2WithDefaults instantiates a new EntriesResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *EntriesResponseV2) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *EntriesResponseV2) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *EntriesResponseV2) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *EntriesResponseV2) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *EntriesResponseV2) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *EntriesResponseV2) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *EntriesResponseV2) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *EntriesResponseV2) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetEntries

`func (o *EntriesResponseV2) GetEntries() []EntryV2`

GetEntries returns the Entries field if non-nil, zero value otherwise.

### GetEntriesOk

`func (o *EntriesResponseV2) GetEntriesOk() (*[]EntryV2, bool)`

GetEntriesOk returns a tuple with the Entries field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEntries

`func (o *EntriesResponseV2) SetEntries(v []EntryV2)`

SetEntries sets Entries field to given value.


### GetEffectiveReference

`func (o *EntriesResponseV2) GetEffectiveReference() ReferenceV2`

GetEffectiveReference returns the EffectiveReference field if non-nil, zero value otherwise.

### GetEffectiveReferenceOk

`func (o *EntriesResponseV2) GetEffectiveReferenceOk() (*ReferenceV2, bool)`

GetEffectiveReferenceOk returns a tuple with the EffectiveReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveReference

`func (o *EntriesResponseV2) SetEffectiveReference(v ReferenceV2)`

SetEffectiveReference sets EffectiveReference field to given value.

### HasEffectiveReference

`func (o *EntriesResponseV2) HasEffectiveReference() bool`

HasEffectiveReference returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


