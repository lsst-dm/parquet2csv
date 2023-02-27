


#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <unordered_map>

struct ParamType
  {
    std::string name;
    std::string pType;
    bool bNotNull;

    ParamType() : name("None"), pType("None"), bNotNull(false){};
    ParamType(std::string n, std::string t, bool bNull=false) : name(n), pType(t), bNotNull(bNull){};
    std::string ToString() const { 
      return (name+" "+pType+" "+(bNotNull? "true" : "false")); 
      }
  };


class PartitionConfig{

  public:
    PartitionConfig() {};
    ~PartitionConfig() = default;
    PartitionConfig(std::string partConfigFile);
    std::string ToString();
    std::vector<std::string> GetConfigParamNames();
    ParamType GetConfigParamType(std::string name);

  private:

    bool endsWith(std::string_view str, std::string_view suffix);
    bool startsWith(std::string_view str, std::string_view prefix);
    void DecodePartitionConfig_json();
    void DecodePartitionConfig_text();

    std::string m_part_config_file;
    std::vector<ParamType> m_paramConfig; 
};

