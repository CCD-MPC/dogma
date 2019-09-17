# dogma
policy engine for verifying conclave workflows.

#input schema

workflows must be valid conclave code, while policies are expected to be of the form: 

```json
{
  "fileName": "<filename>",
  "columns":
  {
    "a":
    {
      "read": true
    },
    "b":
    {
      "read": false
    },
    "c":
    {
      "read": false
    }
  }
}
```
