# NamespaceV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**Elements** | **[]string** |  | 
**Properties** | **map[string]string** |  | 

## Methods

### NewNamespaceV1

`func NewNamespaceV1(elements []string, properties map[string]string, ) *NamespaceV1`

NewNamespaceV1 instantiates a new NamespaceV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewNamespaceV1WithDefaults

`func NewNamespaceV1WithDefaults() *NamespaceV1`

NewNamespaceV1WithDefaults instantiates a new NamespaceV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *NamespaceV1) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *NamespaceV1) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *NamespaceV1) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *NamespaceV1) HasId() bool`

HasId returns a boolean if a field has been set.

### GetElements

`func (o *NamespaceV1) GetElements() []string`

GetElements returns the Elements field if non-nil, zero value otherwise.

### GetElementsOk

`func (o *NamespaceV1) GetElementsOk() (*[]string, bool)`

GetElementsOk returns a tuple with the Elements field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetElements

`func (o *NamespaceV1) SetElements(v []string)`

SetElements sets Elements field to given value.


### GetProperties

`func (o *NamespaceV1) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *NamespaceV1) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *NamespaceV1) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


