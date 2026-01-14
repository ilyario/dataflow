# LogResponseV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**LogEntries** | [**[]LogEntryV1**](LogEntryV1.md) |  | 

## Methods

### NewLogResponseV1

`func NewLogResponseV1(logEntries []LogEntryV1, ) *LogResponseV1`

NewLogResponseV1 instantiates a new LogResponseV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLogResponseV1WithDefaults

`func NewLogResponseV1WithDefaults() *LogResponseV1`

NewLogResponseV1WithDefaults instantiates a new LogResponseV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *LogResponseV1) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *LogResponseV1) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *LogResponseV1) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *LogResponseV1) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *LogResponseV1) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *LogResponseV1) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *LogResponseV1) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *LogResponseV1) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetLogEntries

`func (o *LogResponseV1) GetLogEntries() []LogEntryV1`

GetLogEntries returns the LogEntries field if non-nil, zero value otherwise.

### GetLogEntriesOk

`func (o *LogResponseV1) GetLogEntriesOk() (*[]LogEntryV1, bool)`

GetLogEntriesOk returns a tuple with the LogEntries field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLogEntries

`func (o *LogResponseV1) SetLogEntries(v []LogEntryV1)`

SetLogEntries sets LogEntries field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


