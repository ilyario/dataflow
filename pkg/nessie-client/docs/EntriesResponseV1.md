# EntriesResponseV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**Entries** | [**[]EntryV1**](EntryV1.md) |  | 

## Methods

### NewEntriesResponseV1

`func NewEntriesResponseV1(entries []EntryV1, ) *EntriesResponseV1`

NewEntriesResponseV1 instantiates a new EntriesResponseV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewEntriesResponseV1WithDefaults

`func NewEntriesResponseV1WithDefaults() *EntriesResponseV1`

NewEntriesResponseV1WithDefaults instantiates a new EntriesResponseV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *EntriesResponseV1) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *EntriesResponseV1) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *EntriesResponseV1) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *EntriesResponseV1) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *EntriesResponseV1) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *EntriesResponseV1) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *EntriesResponseV1) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *EntriesResponseV1) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetEntries

`func (o *EntriesResponseV1) GetEntries() []EntryV1`

GetEntries returns the Entries field if non-nil, zero value otherwise.

### GetEntriesOk

`func (o *EntriesResponseV1) GetEntriesOk() (*[]EntryV1, bool)`

GetEntriesOk returns a tuple with the Entries field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEntries

`func (o *EntriesResponseV1) SetEntries(v []EntryV1)`

SetEntries sets Entries field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


