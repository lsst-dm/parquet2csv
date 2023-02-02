

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <map>

struct ParamType
  {
    std::string pType;
    bool bNotNull;

    std::string ToString() const { 
      return (pType+" "+(bNotNull? "true" : "false")); 
      }
  };

class PartitionConfig{

  public:
    PartitionConfig() {};
    ~PartitionConfig() = default;
    PartitionConfig(std::string partConfigFile);
    
  private:

    bool endsWith(std::string_view str, std::string_view suffix);
    bool startsWith(std::string_view str, std::string_view prefix);
    void DecodePartitionConfig_json();
    void DecodePartitionConfig_text();

    std::string m_part_config_file;
    std::map <std::string, ParamType> m_paramConfig; 
};
