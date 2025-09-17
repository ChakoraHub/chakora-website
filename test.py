from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.lang import Builder
from kivy.core.window import Window

# Optional: set a fixed window size for desktop testing
Window.size = (400, 600)

KV = """
<EnquiryScreen>:
    BoxLayout:
        orientation: "vertical"
        padding: 20
        spacing: 15

        Label:
            text: "Enquiry Form"
            font_size: 28
            size_hint_y: None
            height: 50
            color: 0, 0.5, 1, 1  # Light blue heading

        TextInput:
            id: name
            hint_text: "Name"
            multiline: False
            font_size: 18
            padding: [10, 10]

        TextInput:
            id: email
            hint_text: "Email"
            multiline: False
            font_size: 18
            padding: [10, 10]

        TextInput:
            id: phone
            hint_text: "Phone"
            multiline: False
            input_filter: "int"
            font_size: 18
            padding: [10, 10]

        TextInput:
            id: enquiry
            hint_text: "Your Enquiry"
            multiline: True
            font_size: 18
            padding: [10, 10]

        Button:
            text: "Submit"
            size_hint_y: None
            height: 50
            background_color: (0, 0.6, 0, 1)
            font_size: 18
            on_release: root.submit_form()

        Button:
            text: "⬅ Back to Resources"
            size_hint_y: None
            height: 50
            background_color: (0.1, 0.3, 0.9, 1)
            font_size: 18
            on_release: root.go_back()
"""


class EnquiryScreen(Screen):
    def submit_form(self):
        """Collect form data and print/save it"""
        name = self.ids.name.text.strip()
        email = self.ids.email.text.strip()
        phone = self.ids.phone.text.strip()
        enquiry = self.ids.enquiry.text.strip()

        if name and email and phone and enquiry:
            print("✅ New Enquiry Submitted:")
            print(f"Name: {name}")
            print(f"Email: {email}")
            print(f"Phone: {phone}")
            print(f"Enquiry: {enquiry}")
            print("-" * 30)

            # Clear fields after submission
            self.ids.name.text = ""
            self.ids.email.text = ""
            self.ids.phone.text = ""
            self.ids.enquiry.text = ""
        else:
            print("⚠ Please fill all fields before submitting.")

    def go_back(self):
        """Simulate going back to resources"""
        print("⬅ Back to Resources clicked!")


class EnquiryApp(App):
    def build(self):
        Builder.load_string(KV)
        sm = ScreenManager()
        sm.add_widget(EnquiryScreen(name="enquiry"))
        return sm


if __name__ == "__main__":
    EnquiryApp().run()
