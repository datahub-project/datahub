#!/bin/bash -e

#From https://stackoverflow.com/questions/4023830/how-to-compare-two-strings-in-dot-separated-version-format-in-bash
verlte() {
  [  "$1" == "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

brew_install() {
    package=${1}
    required_version=${2}
    printf '\n🔎 Checking if %s installed\n' "${package}"
    version=$(brew list --version|grep "$1"|awk '{ print $2 }')

    if [ -n "${version}" ]; then
      if [ -n "$2" ] && ! verlte "${required_version}" "${version}"; then
        printf '🔽 %s is installed but its version %s is lower than the required %s\n' "${package}" "${version}" "${required_version}. Updating version..."
        brew update && brew upgrade "$1" && printf '✅ %s is installed\n' "${package}"
      else
        printf '✅ %s is already installed\n' "${package} with version ${version}"
      fi
    else
        brew install "$1" && printf '✅ %s is installed\n' "${package}"
    fi
}

arm64_darwin_preflight() {
  printf "✨ Creating/activating Virtual Environment\n"
  python3 -m venv venv
  source venv/bin/activate

  printf "🔎 Checking if Scipy installed\n"
  if pip list | grep -F scipy; then
  	printf "✅ Scipy already installed\n"
  else
  	printf "Scipy not installed\n"
  	printf "⛅ Installing prerequisities for scipy"
  	brew install openblas
  	OPENBLAS="$(brew --prefix openblas)"
  	export OPENBLAS
  	##preinstall numpy and pythran from source
  	pip3 uninstall -y numpy pythran
  	pip3 install cython pybind11
  	pip3 install --no-use-pep517 numpy
  	pip3 install pythran
  	pip3 install --no-use-pep517 scipy
  fi

  printf "✨ Setting up librdkafka prerequisities\n"
  brew_install "librdkafka" "1.9.1"
  brew_install "openssl@1.1"
  brew install "postgresql@14"

  # postgresql installs libs in a strange way
  # we first symlink /opt/postgresql@14 to /opt/postgresql
  if [ ! -z $(brew --prefix)/opt/postgresql ]; then
    printf "✨ Symlinking postgresql@14 to postgresql\n"
    ln -sf $(brew --prefix postgresql@14) $(brew --prefix)/opt/postgres
  fi
  # we then symlink all libs under /opt/postgresql@14/lib/postgresql@14 to /opt/postgresql@14/lib
  if [ ! -z $(brew --prefix postgresql@14)/lib/postgresql@14 ]; then
    printf "✨ Patching up libs in $(brew --prefix postgresql@14)/lib/postgresql@14)\n"
    ln -sf $(brew --prefix postgresql@14)/lib/postgresql@14/* $(brew --prefix postgresql@14)/lib/
  fi

  printf "\e[38;2;0;255;0m✅ Done\e[38;2;255;255;255m\n"

  printf "✨ Setting up environment variable:\n"
  GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
  export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL
  GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
  export GRPC_PYTHON_BUILD_SYSTEM_ZLIB
  CPPFLAGS="-I$(brew --prefix openssl@1.1)/include -I$(brew --prefix librdkafka)/include"
  export CPPFLAGS
  LDFLAGS="-L$(brew --prefix openssl@1.1)/lib -L$(brew --prefix librdkafka)/lib"
  export LDFLAGS
  CPATH="$(brew --prefix librdkafka)/include"
  export CPATH
  C_INCLUDE_PATH="$(brew --prefix librdkafka)/include"
  export C_INCLUDE_PATH
  LIBRARY_PATH="$(brew --prefix librdkafka)/lib"
  export LIBRARY_PATH

cat << EOF
  export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
  export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
  export CPPFLAGS="-I$(brew --prefix openssl@1.1)/include -I$(brew --prefix librdkafka)/include"
  export LDFLAGS="-L$(brew --prefix openssl@1.1)/lib -L$(brew --prefix librdkafka)/lib -L$(brew --prefix postgresql@14)/lib/postgresql@14"
  export CPATH="$(brew --prefix librdkafka)/include"
  export C_INCLUDE_PATH="$(brew --prefix librdkafka)/include"
  export LIBRARY_PATH="$(brew --prefix librdkafka)/lib"

EOF

  if pip list | grep -F confluent-kafka; then
    printf "✅ confluent-kafka already installed\n"
  else
    pip3 install confluent-kafka
  fi

  printf "✨ Setting up prerequisities\n"
  brew install "jq"

  printf "\e[38;2;0;255;0m✅ Done\e[38;2;255;255;255m\n"
}


printf "🔎 Checking if current directory is metadata-ingestion folder\n"
if [ "$(basename "$(pwd)")"	 != "metadata-ingestion" ]; then
	printf "💥 You should run this script in Datahub\'s metadata-ingestion folder but your folder is %s\n" "$(pwd)"
	exit 123
fi
printf '✅ Current folder is metadata-ingestion (%s) folder\n' "$(pwd)"
if [[ $(uname -m) == 'arm64' && $(uname) == 'Darwin' ]]; then
  printf "👟 Running preflight for m1 mac\n"
  arm64_darwin_preflight
fi


printf "\n\e[38;2;0;255;0m✅ Preflight was successful\e[38;2;255;255;255m\n"

