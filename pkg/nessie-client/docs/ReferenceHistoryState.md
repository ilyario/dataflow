# ReferenceHistoryState

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**CommitHash** | Pointer to **string** | Nessie commit ID. | [optional] 
**CommitConsistency** | Pointer to [**CommitConsistency**](CommitConsistency.md) | Consistency status of the commit. | [optional] 
**Meta** | Pointer to [**CommitMeta**](CommitMeta.md) | Meta information from the commit, if available. | [optional] 

## Methods

### NewReferenceHistoryState

`func NewReferenceHistoryState() *ReferenceHistoryState`

NewReferenceHistoryState instantiates a new ReferenceHistoryState object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceHistoryStateWithDefaults

`func NewReferenceHistoryStateWithDefaults() *ReferenceHistoryState`

NewReferenceHistoryStateWithDefaults instantiates a new ReferenceHistoryState object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCommitHash

`func (o *ReferenceHistoryState) GetCommitHash() string`

GetCommitHash returns the CommitHash field if non-nil, zero value otherwise.

### GetCommitHashOk

`func (o *ReferenceHistoryState) GetCommitHashOk() (*string, bool)`

GetCommitHashOk returns a tuple with the CommitHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitHash

`func (o *ReferenceHistoryState) SetCommitHash(v string)`

SetCommitHash sets CommitHash field to given value.

### HasCommitHash

`func (o *ReferenceHistoryState) HasCommitHash() bool`

HasCommitHash returns a boolean if a field has been set.

### GetCommitConsistency

`func (o *ReferenceHistoryState) GetCommitConsistency() CommitConsistency`

GetCommitConsistency returns the CommitConsistency field if non-nil, zero value otherwise.

### GetCommitConsistencyOk

`func (o *ReferenceHistoryState) GetCommitConsistencyOk() (*CommitConsistency, bool)`

GetCommitConsistencyOk returns a tuple with the CommitConsistency field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitConsistency

`func (o *ReferenceHistoryState) SetCommitConsistency(v CommitConsistency)`

SetCommitConsistency sets CommitConsistency field to given value.

### HasCommitConsistency

`func (o *ReferenceHistoryState) HasCommitConsistency() bool`

HasCommitConsistency returns a boolean if a field has been set.

### GetMeta

`func (o *ReferenceHistoryState) GetMeta() CommitMeta`

GetMeta returns the Meta field if non-nil, zero value otherwise.

### GetMetaOk

`func (o *ReferenceHistoryState) GetMetaOk() (*CommitMeta, bool)`

GetMetaOk returns a tuple with the Meta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMeta

`func (o *ReferenceHistoryState) SetMeta(v CommitMeta)`

SetMeta sets Meta field to given value.

### HasMeta

`func (o *ReferenceHistoryState) HasMeta() bool`

HasMeta returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


