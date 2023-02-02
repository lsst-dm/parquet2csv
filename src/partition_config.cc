  // Licensed to the Apache Software Foundation (ASF) under one
  // or more contributor license agreements. See the NOTICE file
  // distributed with this work for additional information
  // regarding copyright ownership. The ASF licenses this file
  // to you under the Apache License, Version 2.0 (the
  // "License"); you may not use this file except in compliance
  // with the License. You may obtain a copy of the License at
  //
  // http://www.apache.org/licenses/LICENSE-2.0

  // Unless required by applicable law or agreed to in writing,
  // software distributed under the License is distributed on an
  // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  // KIND, either express or implied. See the License for the
  // specific language governing permissions and limitations
  // under the License.

  // SES
  // Read the partitioner config files in order to check if the 
  //   parameter vaues are well defined before being send for partitioning
  //   For example: null, NaN, inf, true, false values  <- to be done

  #include "partition_config.h"
  #include <fstream>
  #include <sstream>
  #include <nlohmann/json.hpp>
  using json = nlohmann::json;

  PartitionConfig::PartitionConfig(std::string partConfigFile) : 
      m_part_config_file(partConfigFile)
  {
      if(endsWith(m_part_config_file,std::string(".json")))
        DecodePartitionConfig_json();          

      DecodePartitionConfig_text();   
  }

  bool PartitionConfig::endsWith(std::string_view str, std::string_view suffix)
  {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
  }

  bool PartitionConfig::startsWith(std::string_view str, std::string_view prefix)
  {
    return str.size() >= prefix.size() && 0 == str.compare(0, prefix.size(), prefix);
  }

  void PartitionConfig::DecodePartitionConfig_json()
  {

    std::ifstream f(m_part_config_file);
    json data = json::parse(f);
  }
  
  void PartitionConfig::DecodePartitionConfig_text()
  {
    std::ifstream paramFile;
    paramFile.open(m_part_config_file);
    
    std::string line;
    std::string key;
    
    std::string notNull("NOT NULL");
    while ( paramFile.good() ){
        getline(paramFile, line);

        bool bNotNull=false;
        if(endsWith(line,notNull)) {
            bNotNull=true;
            line=line.substr(0,line.size()-notNull.size());
        }

        ParamType p;
        std::istringstream ss(line);
        ss >> key >> p.pType; // set the variables  
        p.bNotNull=bNotNull;
        m_paramConfig[key] = p; // input them into the map 
    }

    for(const auto& elem : m_paramConfig)
       std::cout << elem.first << " " << elem.second.ToString() << std::endl;
  }
