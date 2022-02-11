package cmd

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestExecute(t *testing.T) {
	readCommand := SpecCommand()
	b := bytes.NewBufferString("")
	readCommand.SetOut(b)
	readCommand.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}

	var specMessage SpecMessage
	err = json.Unmarshal(out, &specMessage)
	assert.Nil(t, err, "should unmarshal spec JSON")
	assert.Equal(t, "SPEC", specMessage.Type)
	assert.NotNil(t, specMessage.Spec)
	spec := specMessage.Spec
	assert.NotNil(t, spec.ConnectionSpecification)

	connectionSpecification := spec.ConnectionSpecification
	assert.Equal(t, "PlanetScale Source Spec", connectionSpecification.Title)

	assert.Equal(t, []string{"host", "database", "username", "password"}, connectionSpecification.Required)

	assert.NotNil(t, connectionSpecification.Properties.Host)
	assert.Equal(t, 0, connectionSpecification.Properties.Host.Order)

	assert.NotNil(t, connectionSpecification.Properties.Database)
	assert.Equal(t, 1, connectionSpecification.Properties.Database.Order)
	assert.NotNil(t, connectionSpecification.Properties.Username)
	assert.Equal(t, 2, connectionSpecification.Properties.Username.Order)
	assert.NotNil(t, connectionSpecification.Properties.Password)
	assert.Equal(t, 3, connectionSpecification.Properties.Password.Order)

	assert.True(t, connectionSpecification.Properties.Password.IsSecret)
}
