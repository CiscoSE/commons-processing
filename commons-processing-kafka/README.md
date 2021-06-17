## commons-processing-kafka

Kafka processing utils.

### Kafka Topic Consumer

Intended to provider some parallelism option and bulk processing for Kafka topic, 
while maintaining the topic offset.
Parallelism and bulk processing can be beneficial to processing like database related actions performance.

Implemented by polling in bulks, while each bulk can be processed in parallel if wanted by consumer handler, with
Kafka commit triggering only after each bulk is processed.
The consumer has a sync retry mechanism.

Working by "at least once" message processing paradigm, which is mostly the case anyway at common consumers implementation,
as message processing can fail before the Kafka commit action. Thus the consumer handler is expected to treat
each message with idempotent outcome.

**Note**: This is not an official Cisco product.

## Future enhancements to consider
* Configurable TopicConsumer parameters via builder.

## Contributions
 * [Contributing](../CONTRIBUTING.md) - how to contribute.
 * [Contributors](../docs/CONTRIBUTORS.md) - Folks who have contributed, thanks very much!

## Licensing

```

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

### Author
Liran Mendelovich

Cisco
