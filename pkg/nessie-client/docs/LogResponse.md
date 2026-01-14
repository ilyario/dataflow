# LogResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**LogEntries** | [**[]LogEntry**](LogEntry.md) |  | 

## Methods

### NewLogResponse

`func NewLogResponse(logEntries []LogEntry, ) *LogResponse`

NewLogResponse instantiates a new LogResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogResponseWithDefaults

`func NewLogResponseWithDefaults() *LogResponse`

NewLogResponseWithDefaults instantiates a new LogResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *LogResponse) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *LogResponse) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *LogResponse) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *LogResponse) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *LogResponse) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *LogResponse) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *LogResponse) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *LogResponse) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetLogEntries

`func (o *LogResponse) GetLogEntries() []LogEntry`

GetLogEntries returns the LogEntries field if non-nil, zero value otherwise.

### GetLogEntriesOk

`func (o *LogResponse) GetLogEntriesOk() (*[]LogEntry, bool)`

GetLogEntriesOk returns a tuple with the LogEntries field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLogEntries

`func (o *LogResponse) SetLogEntries(v []LogEntry)`

SetLogEntries sets LogEntries field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


