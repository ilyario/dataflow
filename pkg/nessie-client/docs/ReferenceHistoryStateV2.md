# ReferenceHistoryStateV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**CommitHash** | Pointer to **string** | Nessie commit ID. | [optional] 
**CommitConsistency** | Pointer to [**CommitConsistencyV2**](CommitConsistencyV2.md) | Consistency status of the commit. | [optional] 
**Meta** | Pointer to [**CommitMetaV2**](CommitMetaV2.md) | Meta information from the commit, if available. | [optional] 

## Methods

### NewReferenceHistoryStateV2

`func NewReferenceHistoryStateV2() *ReferenceHistoryStateV2`

NewReferenceHistoryStateV2 instantiates a new ReferenceHistoryStateV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceHistoryStateV2WithDefaults

`func NewReferenceHistoryStateV2WithDefaults() *ReferenceHistoryStateV2`

NewReferenceHistoryStateV2WithDefaults instantiates a new ReferenceHistoryStateV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCommitHash

`func (o *ReferenceHistoryStateV2) GetCommitHash() string`

GetCommitHash returns the CommitHash field if non-nil, zero value otherwise.

### GetCommitHashOk

`func (o *ReferenceHistoryStateV2) GetCommitHashOk() (*string, bool)`

GetCommitHashOk returns a tuple with the CommitHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitHash

`func (o *ReferenceHistoryStateV2) SetCommitHash(v string)`

SetCommitHash sets CommitHash field to given value.

### HasCommitHash

`func (o *ReferenceHistoryStateV2) HasCommitHash() bool`

HasCommitHash returns a boolean if a field has been set.

### GetCommitConsistency

`func (o *ReferenceHistoryStateV2) GetCommitConsistency() CommitConsistencyV2`

GetCommitConsistency returns the CommitConsistency field if non-nil, zero value otherwise.

### GetCommitConsistencyOk

`func (o *ReferenceHistoryStateV2) GetCommitConsistencyOk() (*CommitConsistencyV2, bool)`

GetCommitConsistencyOk returns a tuple with the CommitConsistency field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitConsistency

`func (o *ReferenceHistoryStateV2) SetCommitConsistency(v CommitConsistencyV2)`

SetCommitConsistency sets CommitConsistency field to given value.

### HasCommitConsistency

`func (o *ReferenceHistoryStateV2) HasCommitConsistency() bool`

HasCommitConsistency returns a boolean if a field has been set.

### GetMeta

`func (o *ReferenceHistoryStateV2) GetMeta() CommitMetaV2`

GetMeta returns the Meta field if non-nil, zero value otherwise.

### GetMetaOk

`func (o *ReferenceHistoryStateV2) GetMetaOk() (*CommitMetaV2, bool)`

GetMetaOk returns a tuple with the Meta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMeta

`func (o *ReferenceHistoryStateV2) SetMeta(v CommitMetaV2)`

SetMeta sets Meta field to given value.

### HasMeta

`func (o *ReferenceHistoryStateV2) HasMeta() bool`

HasMeta returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


