from application.application_initializer import ApplicationInitializer


def main():
    application = ApplicationInitializer().initialize()
    application.run()


if __name__ == "__main__":
    main()
