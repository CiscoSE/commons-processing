package com.cisco.commons.processing;

import lombok.Builder;
import lombok.Getter;

/**
 * Data object.
 * 
 * Contains a logical key for the data object.
 * Multiple data objects representing same data should have the same key.
 * 
 * @author Liran Mendelovich
 * 
 * Copyright 2021 Cisco Systems
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Builder
@Getter
public class DataObject {
	
	private Object key;
	private Object data;
}
