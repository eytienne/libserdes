/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

/**
 * Internal implementation of libserdes C++ interface
 */

#include <cstdarg>

extern "C" {
#include "serdes.h"
};


namespace Serdes {

  class ConfImpl : public Conf {
 public:
    ~ConfImpl () {
      if (conf_)
        serdes_conf_destroy(conf_);
    }

    static Conf *create ();

    ConfImpl (): conf_(NULL), log_cb_(NULL) { }



    ErrorCode set (const std::string &name,
                   const std::string &value,
                   std::string &errstr) {
      char c_errstr[256];
      serdes_err_t err;
      err = serdes_conf_set(conf_, name.c_str(), value.c_str(),
                            c_errstr, sizeof(c_errstr));
      if (err != SERDES_ERR_OK)
        errstr = c_errstr;
      return static_cast<ErrorCode>(err);
    }

    void set (LogCb *log_cb) {
      log_cb_ = log_cb;
    }

    serdes_conf_t *conf_;
    LogCb *log_cb_;

  };


  class HandleImpl : public virtual Handle {
 public:
    ~HandleImpl () {
      if (sd_)
        serdes_destroy(sd_);
    }

    static Handle *create (const Conf *conf, std::string &errstr);

    HandleImpl (): log_cb_(NULL), sd_(NULL) {}

    int schemas_purge (int max_age) {
      return serdes_schemas_purge(sd_, max_age);
    }

    ssize_t serializer_framing_size () const {
      return serdes_serializer_framing_size(sd_);
    }

    ssize_t deserializer_framing_size () const {
      return serdes_deserializer_framing_size(sd_);
    }



    LogCb *log_cb_;
    serdes_t *sd_;
  };


  class SchemaImpl : public Schema {
 public:
    ~SchemaImpl () {
      if (schema_) {
        serdes_schema_set_opaque(schema_, NULL);
        serdes_schema_destroy(schema_);
      }
    }

    SchemaImpl (): schema_(NULL) {}
    SchemaImpl (serdes_schema_t *ss): schema_(ss) {}

    static Schema *get (Handle *handle, int id, std::string &errstr);
    static Schema *get (Handle *handle, std::string &name, std::string &errstr);

    static Schema *add (Handle *handle, int id, std::string &definition,
                        std::string &errstr);
    static Schema *add (Handle *handle, std::string name, std::string &definition,
                        std::string &errstr);
    static Schema *add (Handle *handle, int id, std::string name,
                        std::string &definition, std::string &errstr);


    int id () {
      return serdes_schema_id(schema_);
    }

    const std::string name () {
      const char *name = serdes_schema_name(schema_);
      return std::string(name ? name : "");
    }

    const std::string definition () {
      const char *def = serdes_schema_definition(schema_);
      return std::string(def ? def : "");
    }

    avro::ValidSchema *object () {
      return static_cast<avro::ValidSchema*>(serdes_schema_object(schema_));
    }


    ssize_t framing_write (std::vector<char> &out) const {
      ssize_t framing_size = serdes_serializer_framing_size(serdes_schema_handle(schema_));
      if (framing_size == 0)
        return 0;

      /* Make room for framing */
      int pos = out.size();
      out.resize(out.size() + framing_size);

      /* Write framing */
      return serdes_framing_write(schema_, &out[pos], framing_size);
    }

    serdes_schema_t *schema_;
  };

template <AvroSerializable T>
class AvroImpl : public Avro<T>, public HandleImpl {
 public:
    ~AvroImpl () { }

    static Avro<T> *create (const Conf *conf, std::string &errstr);

    ssize_t serialize (Schema *schema, const T *t,
                       std::vector<char> &out, std::string &errstr);

    ssize_t deserialize (Schema **schemap, T **tp,
                         const void *payload, size_t size, std::string &errstr);

    ssize_t serializer_framing_size () const {
      return HandleImpl::serializer_framing_size();
    }

    ssize_t deserializer_framing_size () const {
      return HandleImpl::deserializer_framing_size();
    }

    int schemas_purge (int max_age) {
      return HandleImpl::schemas_purge(max_age);
    }
};

template <AvroSerializable T>
Serdes::Avro<T>::~Avro () {
}

int create_serdes (HandleImpl *hnd, const Conf *conf, std::string &errstr);

template <AvroSerializable T>
Avro<T> *Avro<T>::create(const Conf *conf, std::string &errstr) {
  AvroImpl<T> *avimpl = new AvroImpl<T>();

  if (create_serdes(avimpl, conf, errstr) == -1) {
    delete avimpl;
    return NULL;
  }

  return avimpl;
};

template <AvroSerializable T>
ssize_t AvroImpl<T>::serialize (Schema *schema, const T *t,
                             std::vector<char> &out, std::string &errstr) {
  auto avro_schema = schema->object();

  /* Binary encoded output stream */
  auto bin_os = avro::memoryOutputStream();
  /* Avro binary encoder */
  auto bin_encoder = avro::validatingEncoder(*avro_schema, avro::binaryEncoder());

  try {
    /* Encode Avro object to Avro binary format */
    bin_encoder->init(*bin_os.get());
    avro::encode(*bin_encoder, *t);
    bin_encoder->flush();

  } catch (const avro::Exception &e) {
    errstr = std::string("Avro serialization failed: ") + e.what();
    return -1;
  }

  /* Extract written bytes. */
  auto encoded = avro::snapshot(*bin_os.get());

  /* Write framing */
  schema->framing_write(out);

  /* Write binary encoded Avro to output vector */
  out.insert(out.end(), encoded->cbegin(), encoded->cend());

  return out.size();
}


template <AvroSerializable T>
ssize_t AvroImpl<T>::deserialize (Schema **schemap, T **tp,
                               const void *payload, size_t size,
                               std::string &errstr) {
  serdes_schema_t *ss;

  /* Read framing */
  char c_errstr[256];
  ssize_t r = serdes_framing_read(sd_, &payload, &size, &ss,
                                  c_errstr, sizeof(c_errstr));
  if (r == -1) {
    errstr = c_errstr;
    return -1;
  } else if (r == 0 && !*schemap) {
    errstr = "Unable to decode payload: No framing and no schema specified";
    return -1;
  }

  Schema *schema = *schemap;
  if (!schema) {
    schema = Serdes::Schema::get(dynamic_cast<HandleImpl*>(this),
                                 serdes_schema_id(ss), errstr);
    if (!schema)
      return -1;
  }

  avro::ValidSchema *avro_schema = schema->object();

  /* Binary input stream */
  auto bin_is = avro::memoryInputStream((const uint8_t *)payload, size);

  /* Binary Avro decoder */
  avro::DecoderPtr bin_decoder = avro::validatingDecoder(*avro_schema,
                                                         avro::binaryDecoder());

  T *t = new T;

  try {
    /* Decode binary to Avro object */
    bin_decoder->init(*bin_is);
    avro::decode(*bin_decoder, *t);

  } catch (const avro::Exception &e) {
    errstr = std::string("Avro deserialization failed: ") + e.what();
    delete t;
    return -1;
  }

  *schemap = schema;
  *tp = t;
  return 0;
}

}
