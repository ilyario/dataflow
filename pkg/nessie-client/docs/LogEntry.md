# LogEntry

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**CommitMeta** | [**CommitMeta**](CommitMeta.md) |  | 
**AdditionalParents** | Pointer to **[]string** |  | [optional] 
**ParentCommitHash** | Pointer to **string** |  | [optional] 
**Operations** | Pointer to [**[]Operation**](Operation.md) |  | [optional] 

## Methods

### NewLogEntry

`func NewLogEntry(commitMeta CommitMeta, ) *LogEntry`

NewLogEntry instantiates a new LogEntry object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogEntryWithDefaults

`func NewLogEntryWithDefaults() *LogEntry`

NewLogEntryWithDefaults instantiates a new LogEntry object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCommitMeta

`func (o *LogEntry) GetCommitMeta() CommitMeta`

GetCommitMeta returns the CommitMeta field if non-nil, zero value otherwise.

### GetCommitMetaOk

`func (o *LogEntry) GetCommitMetaOk() (*CommitMeta, bool)`

GetCommitMetaOk returns a tuple with the CommitMeta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMeta

`func (o *LogEntry) SetCommitMeta(v CommitMeta)`

SetCommitMeta sets CommitMeta field to given value.


### GetAdditionalParents

`func (o *LogEntry) GetAdditionalParents() []string`

GetAdditionalParents returns the AdditionalParents field if non-nil, zero value otherwise.

### GetAdditionalParentsOk

`func (o *LogEntry) GetAdditionalParentsOk() (*[]string, bool)`

GetAdditionalParentsOk returns a tuple with the AdditionalParents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAdditionalParents

`func (o *LogEntry) SetAdditionalParents(v []string)`

SetAdditionalParents sets AdditionalParents field to given value.

### HasAdditionalParents

`func (o *LogEntry) HasAdditionalParents() bool`

HasAdditionalParents returns a boolean if a field has been set.

### GetParentCommitHash

`func (o *LogEntry) GetParentCommitHash() string`

GetParentCommitHash returns the ParentCommitHash field if non-nil, zero value otherwise.

### GetParentCommitHashOk

`func (o *LogEntry) GetParentCommitHashOk() (*string, bool)`

GetParentCommitHashOk returns a tuple with the ParentCommitHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetParentCommitHash

`func (o *LogEntry) SetParentCommitHash(v string)`

SetParentCommitHash sets ParentCommitHash field to given value.

### HasParentCommitHash

`func (o *LogEntry) HasParentCommitHash() bool`

HasParentCommitHash returns a boolean if a field has been set.

### GetOperations

`func (o *LogEntry) GetOperations() []Operation`

GetOperations returns the Operations field if non-nil, zero value otherwise.

### GetOperationsOk

`func (o *LogEntry) GetOperationsOk() (*[]Operation, bool)`

GetOperationsOk returns a tuple with the Operations field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperations

`func (o *LogEntry) SetOperations(v []Operation)`

SetOperations sets Operations field to given value.

### HasOperations

`func (o *LogEntry) HasOperations() bool`

HasOperations returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


