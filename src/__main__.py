from dependency_injection.containers import ApplicationProviders, RepetitiveExecutionProviders


def main():
    #ApplicationProviders.application().run()
    RepetitiveExecutionProviders.repetitive_execution().run()


if __name__ == "__main__":
    main()
