# LogResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**LogEntries** | [**[]LogEntryV2**](LogEntryV2.md) |  | 

## Methods

### NewLogResponseV2

`func NewLogResponseV2(logEntries []LogEntryV2, ) *LogResponseV2`

NewLogResponseV2 instantiates a new LogResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogResponseV2WithDefaults

`func NewLogResponseV2WithDefaults() *LogResponseV2`

NewLogResponseV2WithDefaults instantiates a new LogResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *LogResponseV2) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *LogResponseV2) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *LogResponseV2) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *LogResponseV2) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *LogResponseV2) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *LogResponseV2) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *LogResponseV2) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *LogResponseV2) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetLogEntries

`func (o *LogResponseV2) GetLogEntries() []LogEntryV2`

GetLogEntries returns the LogEntries field if non-nil, zero value otherwise.

### GetLogEntriesOk

`func (o *LogResponseV2) GetLogEntriesOk() (*[]LogEntryV2, bool)`

GetLogEntriesOk returns a tuple with the LogEntries field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLogEntries

`func (o *LogResponseV2) SetLogEntries(v []LogEntryV2)`

SetLogEntries sets LogEntries field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


