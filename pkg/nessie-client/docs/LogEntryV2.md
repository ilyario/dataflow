# LogEntryV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**CommitMeta** | [**CommitMetaV2**](CommitMetaV2.md) |  | 
**ParentCommitHash** | Pointer to **string** |  | [optional] 
**Operations** | Pointer to [**[]OperationV2**](OperationV2.md) |  | [optional] 

## Methods

### NewLogEntryV2

`func NewLogEntryV2(commitMeta CommitMetaV2, ) *LogEntryV2`

NewLogEntryV2 instantiates a new LogEntryV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogEntryV2WithDefaults

`func NewLogEntryV2WithDefaults() *LogEntryV2`

NewLogEntryV2WithDefaults instantiates a new LogEntryV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCommitMeta

`func (o *LogEntryV2) GetCommitMeta() CommitMetaV2`

GetCommitMeta returns the CommitMeta field if non-nil, zero value otherwise.

### GetCommitMetaOk

`func (o *LogEntryV2) GetCommitMetaOk() (*CommitMetaV2, bool)`

GetCommitMetaOk returns a tuple with the CommitMeta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMeta

`func (o *LogEntryV2) SetCommitMeta(v CommitMetaV2)`

SetCommitMeta sets CommitMeta field to given value.


### GetParentCommitHash

`func (o *LogEntryV2) GetParentCommitHash() string`

GetParentCommitHash returns the ParentCommitHash field if non-nil, zero value otherwise.

### GetParentCommitHashOk

`func (o *LogEntryV2) GetParentCommitHashOk() (*string, bool)`

GetParentCommitHashOk returns a tuple with the ParentCommitHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetParentCommitHash

`func (o *LogEntryV2) SetParentCommitHash(v string)`

SetParentCommitHash sets ParentCommitHash field to given value.

### HasParentCommitHash

`func (o *LogEntryV2) HasParentCommitHash() bool`

HasParentCommitHash returns a boolean if a field has been set.

### GetOperations

`func (o *LogEntryV2) GetOperations() []OperationV2`

GetOperations returns the Operations field if non-nil, zero value otherwise.

### GetOperationsOk

`func (o *LogEntryV2) GetOperationsOk() (*[]OperationV2, bool)`

GetOperationsOk returns a tuple with the Operations field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperations

`func (o *LogEntryV2) SetOperations(v []OperationV2)`

SetOperations sets Operations field to given value.

### HasOperations

`func (o *LogEntryV2) HasOperations() bool`

HasOperations returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


