# OperationV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | [**ContentKeyV1**](ContentKeyV1.md) |  | 
**Content** | [**ContentV1**](ContentV1.md) |  | 
**ExpectedContent** | Pointer to [**ContentV1**](ContentV1.md) |  | [optional] 

## Methods

### NewOperationV1

`func NewOperationV1(key ContentKeyV1, content ContentV1, ) *OperationV1`

NewOperationV1 instantiates a new OperationV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewOperationV1WithDefaults

`func NewOperationV1WithDefaults() *OperationV1`

NewOperationV1WithDefaults instantiates a new OperationV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *OperationV1) GetKey() ContentKeyV1`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *OperationV1) GetKeyOk() (*ContentKeyV1, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *OperationV1) SetKey(v ContentKeyV1)`

SetKey sets Key field to given value.


### GetContent

`func (o *OperationV1) GetContent() ContentV1`

GetContent returns the Content field if non-nil, zero value otherwise.

### GetContentOk

`func (o *OperationV1) GetContentOk() (*ContentV1, bool)`

GetContentOk returns a tuple with the Content field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContent

`func (o *OperationV1) SetContent(v ContentV1)`

SetContent sets Content field to given value.


### GetExpectedContent

`func (o *OperationV1) GetExpectedContent() ContentV1`

GetExpectedContent returns the ExpectedContent field if non-nil, zero value otherwise.

### GetExpectedContentOk

`func (o *OperationV1) GetExpectedContentOk() (*ContentV1, bool)`

GetExpectedContentOk returns a tuple with the ExpectedContent field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedContent

`func (o *OperationV1) SetExpectedContent(v ContentV1)`

SetExpectedContent sets ExpectedContent field to given value.

### HasExpectedContent

`func (o *OperationV1) HasExpectedContent() bool`

HasExpectedContent returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


