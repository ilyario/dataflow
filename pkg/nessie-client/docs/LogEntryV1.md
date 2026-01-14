# LogEntryV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**CommitMeta** | [**CommitMetaV1**](CommitMetaV1.md) |  | 
**AdditionalParents** | Pointer to **[]string** |  | [optional] 
**ParentCommitHash** | Pointer to **string** |  | [optional] 
**Operations** | Pointer to [**[]OperationV1**](OperationV1.md) |  | [optional] 

## Methods

### NewLogEntryV1

`func NewLogEntryV1(commitMeta CommitMetaV1, ) *LogEntryV1`

NewLogEntryV1 instantiates a new LogEntryV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogEntryV1WithDefaults

`func NewLogEntryV1WithDefaults() *LogEntryV1`

NewLogEntryV1WithDefaults instantiates a new LogEntryV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCommitMeta

`func (o *LogEntryV1) GetCommitMeta() CommitMetaV1`

GetCommitMeta returns the CommitMeta field if non-nil, zero value otherwise.

### GetCommitMetaOk

`func (o *LogEntryV1) GetCommitMetaOk() (*CommitMetaV1, bool)`

GetCommitMetaOk returns a tuple with the CommitMeta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMeta

`func (o *LogEntryV1) SetCommitMeta(v CommitMetaV1)`

SetCommitMeta sets CommitMeta field to given value.


### GetAdditionalParents

`func (o *LogEntryV1) GetAdditionalParents() []string`

GetAdditionalParents returns the AdditionalParents field if non-nil, zero value otherwise.

### GetAdditionalParentsOk

`func (o *LogEntryV1) GetAdditionalParentsOk() (*[]string, bool)`

GetAdditionalParentsOk returns a tuple with the AdditionalParents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalParents

`func (o *LogEntryV1) SetAdditionalParents(v []string)`

SetAdditionalParents sets AdditionalParents field to given value.

### HasAdditionalParents

`func (o *LogEntryV1) HasAdditionalParents() bool`

HasAdditionalParents returns a boolean if a field has been set.

### GetParentCommitHash

`func (o *LogEntryV1) GetParentCommitHash() string`

GetParentCommitHash returns the ParentCommitHash field if non-nil, zero value otherwise.

### GetParentCommitHashOk

`func (o *LogEntryV1) GetParentCommitHashOk() (*string, bool)`

GetParentCommitHashOk returns a tuple with the ParentCommitHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetParentCommitHash

`func (o *LogEntryV1) SetParentCommitHash(v string)`

SetParentCommitHash sets ParentCommitHash field to given value.

### HasParentCommitHash

`func (o *LogEntryV1) HasParentCommitHash() bool`

HasParentCommitHash returns a boolean if a field has been set.

### GetOperations

`func (o *LogEntryV1) GetOperations() []OperationV1`

GetOperations returns the Operations field if non-nil, zero value otherwise.

### GetOperationsOk

`func (o *LogEntryV1) GetOperationsOk() (*[]OperationV1, bool)`

GetOperationsOk returns a tuple with the Operations field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperations

`func (o *LogEntryV1) SetOperations(v []OperationV1)`

SetOperations sets Operations field to given value.

### HasOperations

`func (o *LogEntryV1) HasOperations() bool`

HasOperations returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


