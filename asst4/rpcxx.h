// -*- c++ -*-
#ifndef RPCXX_H
#define RPCXX_H

#include <cstdlib>
#include "rpc.h"
#include <iostream>
#include <typeinfo>
#include <assert.h>

namespace rpc
{

  // Protocol is used for encode and decode a type to/from the network.
  //
  // You may use network byte order, but it's optional. We won't test your code
  // on two different architectures.

  // TASK1: add more specializations to Protocol template class to support more
  // types.
  template <typename T>
  struct Protocol
  {

    /* out_bytes: Write data into this buffer. It's size is equal to *out_len
    *   out_len: Initially, *out_len tells you the size of the buffer out_bytes.
    *            However, it is also used as a return value, so you must set *out_len
    *            to the number of bytes you wrote to the buffer.
    *         x: the data you want to write to buffer
    */

    static constexpr size_t TYPE_SIZE = sizeof(T);
    static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const T &x)
    {
      if (*out_len < TYPE_SIZE)
      {
        return false;
      }

      memcpy(out_bytes, &x, TYPE_SIZE);

      *out_len = TYPE_SIZE;
      return true;
    }

    /* in_bytes: Read data from this buffer. It's size is equal to *in_len
   *   in_len: Initially, *in_len tells you the size of the buffer in_bytes.
   *           However, it is also used as a return value, so you must set *in_len
   *           to the number of bytes you consume from the buffer.
   *        x: the data you want to read from the buffer
   */
    static bool Decode(uint8_t *in_bytes, uint32_t *in_len, bool *ok, T &x)
    {
      if (*in_len < TYPE_SIZE)
      {
        return false;
      }
      memcpy(&x, in_bytes, TYPE_SIZE);
      *in_len = TYPE_SIZE;
      return true;
    }
  };

  template <>
  struct Protocol<std::string>
  {
    static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const std::string &x)
    {
      std::cout<< "ENCODING ..." <<std::endl;
      if (*out_len < x.length() + 1)
      {
        return false;
      }

      char * c_string = new char[x.length() + 1];
      strcpy(c_string, x.c_str());
      // assert(sizeof(c_string) == x.length() + 1);
      memcpy(out_bytes, c_string, (x.length() + 1)*sizeof(char));

      std::cout << "Sizes: "<< sizeof(c_string) << " " << x.length() + 1<<std::endl;
      std::cout << "Encoded: " << c_string <<std::endl;
      *out_len = (x.length() + 1);
      delete c_string;
      return true;
    }

    /* in_bytes: Read data from this buffer. It's size is equal to *in_len
   *   in_len: Initially, *in_len tells you the size of the buffer in_bytes.
   *           However, it is also used as a return value, so you must set *in_len
   *           to the number of bytes you consume from the buffer.
   *        x: the data you want to read from the buffer
   */
    static bool Decode(uint8_t *in_bytes, uint32_t *in_len, bool *ok, std::string &x)
    {
      std::cout<<"DECODING with in_len"<< *in_len <<std::endl;
      x.clear();
      bool end = false;
      int null_index=0;
      for (int i =0; i < *in_len; i++)
      {

        if (in_bytes[i] == '\0')
        {
          end = true;
          null_index = i;
          break;
        }
      }

      if (!end)
      {
        x.clear();
        return false;
      }

      std::cout << "null found: " << end << ", null index: " << null_index<< std::endl;

      for (int i = 0 ; i < *in_len ; i++){
        if(in_bytes[i] == '\0'){
          break;
        }
        x.push_back(in_bytes[i]);
      }
      std::cout << "Final string: " << x <<std::endl;
      *in_len = x.length() + 1;
      return true;
    }
  };

  //~~~~~ TASK2: Client-side ~~~~~
  class IntParam : public BaseParams
  {
    int p;

  public:
    IntParam(int p) : p(p) {}

    bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override
    {
      return Protocol<int>::Encode(out_bytes, out_len, p);
    }
  };

  template <typename T>
  class Param : public BaseParams
  {
    T p;

  public:
    Param() {}
    Param(T p) : p(p) {}

    bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override
    {
      return Protocol<T>::Encode(out_bytes, out_len, p);
    }
  };

  template <typename T, typename S>
  class TwoParam : public BaseParams
  {
    T p;
    S q;

  public:
    TwoParam() {}
    TwoParam(T p, S q) : p(p), q(q) {}

    bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override
    {
      uint32_t inti_len = *out_len;
      if(!Protocol<T>::Encode(out_bytes, out_len, p)){
        return false;
      }
      uint32_t consumed = *out_len;
      *out_len = inti_len - consumed;

      if(!Protocol<S>::Encode(out_bytes + consumed, out_len, q)){
        return false;
      }

      *out_len += consumed;
      return true;
    }
  };

  template <>
  class Param<void> : public BaseParams
  {
  public:
    Param() {}

    bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override
    {
      *out_len = 0;
      return true;
    }
  };

  // TASK2: Server-side

  //~~~~~ Procedure ~~~~~
  template <typename Svc>
  class IntIntProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      // std::cout << "Decoding and Executing" <<std::endl;
      int x;
      // This function is similar to Decode. We need to return false if buffer
      // isn't large enough, or fatal error happens during parsing.
      if (!Protocol<int>::Decode(in_bytes, in_len, ok, x) || !*ok)
      {
        return false;
      }
      // Now we cast the function pointer func_ptr to its original type.
      //
      // This incomplete solution only works for this type of member functions.
      using FunctionPointerType = int (Svc::*)(int);
      auto p = func_ptr.To<FunctionPointerType>();
      int result = (((Svc *)instance)->*p)(x);
      if (!Protocol<int>::Encode(out_bytes, out_len, result))
      {
        // out_len should always be large enough so this branch shouldn't be
        // taken. However just in case, we return an fatal error by setting *ok
        // to false.
        *ok = false;
        return false;
      }
      return true;
    }
  };

  // Void function(void)
  template <typename Svc>
  class VoidReturnProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      *in_len = 0;
      *out_len = 0;
      using FunctionPointerType = void (Svc::*)();
      auto p = func_ptr.To<FunctionPointerType>();
      (((Svc *)instance)->*p)();
      return true;
    }
  };

  // RT function()
  template <typename Svc, typename RT>
  class NoParamProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      // Nothing to decode!
      *in_len = 0;

      // Now we cast the function pointer func_ptr to its original type.
      using FunctionPointerType = RT (Svc::*)();
      auto p = func_ptr.To<FunctionPointerType>();
      RT result = (((Svc *)instance)->*p)();
      if (!Protocol<RT>::Encode(out_bytes, out_len, result))
      {
        *ok = false;
        return false;
      }
      return true;
    }
  };

  // RT function(A1)
  template <typename Svc, typename RT, typename A1>
  class OneParamProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      A1 x;
      // This function is similar to Decode. We need to return false if buffer
      // isn't large enough, or fatal error happens during parsing.
      if (!Protocol<A1>::Decode(in_bytes, in_len, ok, x) || !*ok)
      {
        return false;
      }

      using FunctionPointerType = RT (Svc::*)(A1);
      auto p = func_ptr.To<FunctionPointerType>();
      RT result = (((Svc *)instance)->*p)(x);
      if (!Protocol<RT>::Encode(out_bytes, out_len, result))
      {
        // out_len should always be large enough so this branch shouldn't be
        // taken. However just in case, we return an fatal error by setting *ok
        // to false.
        *ok = false;
        return false;
      }
      return true;
    }
  };

  // RT function(A1, A2)
  template <typename Svc, typename RT, typename A1, typename A2>
  class TwoParamProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      A1 x;
      A2 y;
      
      uint32_t og_in_len = *in_len;
      // This function is similar to Decode. We need to return false if buffer
      // isn't large enough, or fatal error happens during parsing.
      if (!Protocol<A1>::Decode(in_bytes, in_len, ok, x) || !*ok)
      {
        return false;
      }
      uint32_t consumed = *in_len;
      *in_len = og_in_len - consumed;

      if (!Protocol<A2>::Decode(in_bytes + consumed, in_len, ok, y) || !*ok)
      {
        return false;
      }
      *in_len += consumed;

      using FunctionPointerType = RT (Svc::*)(A1, A2);
      auto p = func_ptr.To<FunctionPointerType>();
      RT result = (((Svc *)instance)->*p)(x, y);
      if (!Protocol<RT>::Encode(out_bytes, out_len, result))
      {
        // out_len should always be large enough so this branch shouldn't be
        // taken. However just in case, we return an fatal error by setting *ok
        // to false.
        *ok = false;
        return false;
      }
      return true;
    }
  };

  template <typename Svc, typename RT, typename A1, typename A2>
  class VoidTwoParamProcedure : public BaseProcedure
  {
    bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                          uint8_t *out_bytes, uint32_t *out_len,
                          bool *ok) override final
    {
      A1 x;
      A2 y;
      
      uint32_t og_in_len = *in_len;
      // This function is similar to Decode. We need to return false if buffer
      // isn't large enough, or fatal error happens during parsing.
      if (!Protocol<A1>::Decode(in_bytes, in_len, ok, x) || !*ok)
      {
        return false;
      }
      uint32_t consumed = *in_len;
      *in_len = og_in_len - consumed;

      if (!Protocol<A2>::Decode(in_bytes + consumed, in_len, ok, y) || !*ok)
      {
        return false;
      }
      *in_len += consumed;

      using FunctionPointerType = RT (Svc::*)(A1, A2);
      auto p = func_ptr.To<FunctionPointerType>();
      (((Svc *)instance)->*p)(x, y);
      *out_len = 0;
      return true;
    }
  };

  //~~~~~~~~~~~~~~~~~~~~~

  // TASK2: Client-side
  class IntResult : public BaseResult
  {
    int r;

  public:
    bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len, bool *ok) override final
    {
      return Protocol<int>::Decode(in_bytes, in_len, ok, r);
    }
    int &data() { return r; }
  };

  template <typename T>
  class Result : public BaseResult
  {
    T r;
  public:
    bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len, bool *ok) override final
    {
      return Protocol<T>::Decode(in_bytes, in_len, ok, r);
    }

    T &data() { return r; }
  };

  template <>
  class Result<void> : public BaseResult
  {
    public:

      bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len, bool *ok) override final
    {
      *in_len = 0;
      return true;
    }
  };

  // TASK2: Client-side
  class Client : public BaseClient
  {

  // Templating is left as an exercise to the teaching staff
  public:
    template <typename Svc>
    IntResult *Call(Svc *svc, int (Svc::*func)(int), int x)
    {
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(func));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new IntResult();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new IntParam(x), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }

    template <typename Svc, typename RT, typename... FA>
    Result<RT> *Call(Svc *svc, RT (Svc::*f)(FA...), ...)
    {
      std::cout << "WARNING: Calling "
                << typeid(decltype(f)).name()
                << " is not supported\n";
      return nullptr;
    }

    // bool function(void)
    template <typename Svc>
    Result<bool> *Call(Svc *svc, bool (Svc::*f)(void))
    {
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<bool>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new Param<bool>(), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }

    // void function(void)
    template <typename Svc>
    Result<void> *Call(Svc *svc, void (Svc::*f)(void))
    {
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<void>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new Param<void>(), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }


    template <typename Svc>
    Result<std::string> *Call(Svc *svc, std::string (Svc::*f)(unsigned int), unsigned int x)
    {
      std::cout << "Call std::string <- unsigned int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<std::string>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new Param<unsigned int>(x), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }

      std::cout<<std::endl<<"BIG COCK ---->  " << result->data()<<std::endl;
      return result;
    }

    template <typename Svc, typename RT, typename A1>
    Result<RT> *Call(Svc *svc, RT (Svc::*f)(A1), A1 x)
    {
      std::cout << "Call std::string <- unsigned int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<RT>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new Param<A1>(x), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }

      std::cout<<std::endl<<"BIG COCK ---->  " << result->data()<<std::endl;
      return result;
    }

    template <typename Svc>
    Result<std::string> *Call(Svc *svc, std::string (Svc::*f)(std::string, int), std::string x, int y)
    {
      std::cout << "Call std::string <- std::string int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<std::string>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new TwoParam<std::string, int>(x, y), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }

    template <typename Svc>
    Result<std::string> *Call(Svc *svc, std::string (Svc::*f)(std::string), std::string x)
    {
      std::cout << "Call std::string <- std::string int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<std::string>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new Param<std::string>(x), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }

    template <typename Svc>
    Result<void> *Call(Svc *svc, void (Svc::*f)(std::string, std::string), std::string x, std::string y)
    {
      std::cout << "Call std::string <- std::string int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<void>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new TwoParam<std::string, std::string>(x, y), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }

    template <typename Svc>
    Result<unsigned long> *Call(Svc *svc, unsigned long (Svc::*f)(int, unsigned int), int x, unsigned int y)
    {
      std::cout << "Call std::string <- std::string int" << std::endl;
      // Lookup instance and function IDs.
      int instance_id = svc->instance_id();
      int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(f));

      // This incomplete solution only works for this type of member functions.
      // So the result must be an integer.
      auto result = new Result<unsigned long>();

      // We also send the paramters of the functions. For this incomplete
      // solution, it must be one integer.
      if (!Send(instance_id, func_id, new TwoParam<int, unsigned int>(x, y), result))
      {
        // Fail to send, then delete the result and return nullptr.
        delete result;
        return nullptr;
      }
      return result;
    }
  };

  // TASK2: Server-side
  template <typename Svc>
  class Service : public BaseService
  {
  protected:
    void Export(int (Svc::*func)(int))
    {
      std::cout << "Exporting int function(int)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new IntIntProcedure<Svc>());
    }

    void Export(void (Svc::*func)(void))
    {
      std::cout << "Exporting void function(void)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new VoidReturnProcedure<Svc>());
    }

    void Export(bool (Svc::*func)(void))
    {
      std::cout << "Exporting bool function(void)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new NoParamProcedure<Svc, bool>());
    }

    void Export(std::string (Svc::*func)(unsigned int))
    {
      std::cout << "Exporting std::string function(unsigned int)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new OneParamProcedure<Svc, std::string, unsigned int>());
    }

    void Export(std::string (Svc::*func)(std::string))
    {
      std::cout << "Exporting std::string function(std::string)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new OneParamProcedure<Svc, std::string, std::string>());
    }

    void Export(std::string (Svc::*func)(std::string, int))
    {
      std::cout << "Exporting std::string function(std::string, int)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new TwoParamProcedure<Svc, std::string, std::string, int>());
    }

    void Export(unsigned int (Svc::*func)(std::string, int))
    {
      std::cout << "Exporting unsigned int function(std::string, unsigned int)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new TwoParamProcedure<Svc, unsigned int, std::string, int>());
    }

    void Export(void (Svc::*func)(std::string, int))
    {
      std::cout << "Exporting void function(std::string, int)" << std::endl;
      // ExportRaw(MemberFunctionPtr::From(func), new TwoParamProcedure<Svc, unsigned int, std::string, int>());
    }

    void Export(void (Svc::*func)(std::string, std::string))
    {
      std::cout << "Exporting void function(std::string, std::string)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new VoidTwoParamProcedure<Svc, void, std::string, std::string>());
    }

    void Export(unsigned long (Svc::*func)(int, unsigned int))
    {
      std::cout << "Exporting unsinged long function(int ,unsigned int)" << std::endl;
      ExportRaw(MemberFunctionPtr::From(func), new TwoParamProcedure<Svc, unsigned long, int, unsigned int>());
    }

    template <typename MemberFunction>
    void Export(MemberFunction f)
    {
      std::cout << "WARNING: Exporting "
                << typeid(MemberFunction).name()
                << " is not supported\n";
    }
  };

} // namespace rpc

#endif /* RPCXX_H */
